open Batteries
open Saturn


module type DefaultType = sig
  type t
  val default: unit -> t
  val to_string: t -> string
end

module SafeLinkedList (H: Hashtbl.HashedType) (D: DefaultType) = struct
  type t = {
    key: H.t;
    value: D.t Atomic.t;
    next: t option Atomic.t;
  }

  let create key = {
    key = key;
    value = Atomic.make @@ D.default ();
    next = Atomic.make None;
  }

  let create_with_value key value = {
    key = key;
    value = value;
    next = Atomic.make None;
  }

  let create_with_value_and_next key value next = {
    key = key;
    value = value;
    next = Atomic.make (Some next);
  }

  let find_option sll key =
    let rec find sll key =
      if H.equal sll.key key then Some sll.value
      else match Atomic.get sll.next with
        | None -> None
        | Some next -> find next key
    in
    find sll key

  let find sll key =
    match find_option sll key with
    | None -> failwith "Key not found"
    | Some value -> value

  let rec get_create sll key =
    if H.equal sll.key key then (sll.value, false)
    else (
      match Atomic.get sll.next with
      | None ->
        let new_sll = {
          key = key;
          value = Atomic.make @@ D.default (); 
          next = Atomic.make None;
        } in
        let success = Atomic.compare_and_set sll.next None (Some new_sll) in
        if success then (new_sll.value, true)
        else get_create sll key 
      | Some next -> get_create next key 
    )

  let rec insert_value sll key value =
    if H.equal sll.key key then ()
    else (
      match Atomic.get sll.next with
      | None ->
        let new_sll = {
          key = key;
          value = value;
          next = Atomic.make None;
        } in
        let success = Atomic.compare_and_set sll.next None (Some new_sll) in
        if not success then insert_value sll key value
      | Some next -> insert_value next key value
    )

  let to_list sll =
    let rec aux acc sll =
      match sll with
      | None -> acc
      | Some sll ->
        aux (acc @ [(sll.key, sll.value)]) (Atomic.get sll.next)
    in
    aux [] (Some sll)

  let to_seq sll =
    let rec aux sll () =
      match sll with
      | None -> Seq.Nil
      | Some sll -> Seq.Cons ((sll.key, sll.value), aux (Atomic.get sll.next))
    in aux (Some sll)

  let to_string sll =
    let rec aux acc sll =
      match sll with
      | None -> acc
      | Some sll ->
        aux (acc ^ D.to_string (Atomic.get sll.value) ^ " ") (Atomic.get sll.next)
    in
    aux "" (Some sll)
end

module SafeHashmapSat (H: Hashtbl.HashedType) (D: DefaultType) (HM:Hashtbl.S with type key = H.t) = struct
  type t = (H.t, D.t Atomic.t) Htbl.t

  let create () = Htbl.create ~hashed_type:(module H) ()

  let to_seq = Htbl.to_seq
  let to_list hm = to_seq hm |> List.of_seq

  let find_option = Htbl.find_opt
  let find = Htbl.find_exn
  let mem = Htbl.mem

  let find_create (hm : t) (key : H.t) =
    let found_val = Htbl.find_opt hm key in
    match found_val with 
    | Some found_val -> (found_val, false)
    | None -> begin
        let new_val = Atomic.make @@ D.default () in
        let added = Htbl.try_add hm key new_val in
        if added then (new_val, true) else (Htbl.find_exn hm key, false)
      end

  let to_hashtbl hm =
    let ht = HM.create 10 in
    let seq = to_seq hm in
    Seq.iter (fun (k, v) -> HM.add ht k (Atomic.get v)) seq;
    ht

end

module SafeHashmap (H: Hashtbl.HashedType) (D: DefaultType) (HM:Hashtbl.S with type key = H.t) = struct
  module Bucket = SafeLinkedList(H)(D)

  type t = {
    size: int Atomic.t;
    nr_elements: int Atomic.t;
    resize_generation: int Atomic.t;
    buckets: Bucket.t option Atomic.t array Atomic.t;
  }

  let create () = 
    let size = 100 in
    {
      size = Atomic.make size;
      nr_elements = Atomic.make 0;
      resize_generation = Atomic.make 0;
      buckets = Atomic.make @@ Array.init size (fun _ -> Atomic.make None);
    }

  let to_list hm =
    Array.fold_left (fun acc bucket ->
        match Atomic.get bucket with
        | None -> acc
        | Some bucket -> acc @ Bucket.to_list bucket
      ) [] (Atomic.get hm.buckets)

  let to_seq hm =
    let bucket_seq = Array.to_seq (Atomic.get hm.buckets) in
    let non_atomic_bucket_seq = Seq.map Atomic.get bucket_seq in
    let non_option_bucket_seq = Seq.filter (fun x -> x <> None) non_atomic_bucket_seq in
    let non_atomic_bucket_seq = Seq.map (fun x -> match x with | Some x -> x | None -> failwith "This should not happen") non_option_bucket_seq in
    Seq.flat_map Bucket.to_seq non_atomic_bucket_seq

  (* let to_string hm = *)
  (*   let as_list = to_list hm in *)
  (*   List.fold_left (fun acc x -> acc ^ H.to_string (fst x) ^ ":" ^ D.to_string (snd x) ^ " ") "" as_list *)

  let find_option hm key =
    let hash = abs @@ H.hash key in
    let bucket = Array.get (Atomic.get hm.buckets) (hash mod (Atomic.get hm.size)) in
    match Atomic.get bucket with
    | None -> None
    | Some bucket -> Bucket.find_option bucket key

  let find hm key =
    match find_option hm key with
    | None -> failwith "Key not found"
    | Some value -> value

  let mem hm key =
    match find_option hm key with
    | None -> false
    | Some _ -> true


  let rec find_create (hm : t) (key : H.t) =
    let rec find_create_inner hm key hash buckets =
      (* Logs.error "Accessing index %d" (hash mod (Atomic.get hm.size)); *)
      let bucket = Array.get buckets (hash mod (Atomic.get hm.size)) in
      match Atomic.get bucket with
      | None ->
        let new_bucket = Bucket.create key in
        let success = Atomic.compare_and_set bucket None (Some new_bucket) in
        if success then (
          (* Atomic.incr hm.nr_elements; *)
          (new_bucket.value, true) 
        )
        else find_create_inner hm key hash buckets 
      | Some bucket -> 
        let value, was_created = Bucket.get_create bucket key in
        (* if was_created then Atomic.incr hm.nr_elements; *)
        (value, was_created)
    in
    let current_generation = Atomic.get hm.resize_generation in
    let hash = abs @@ H.hash key in
    let value, was_created = find_create_inner hm key hash (Atomic.get hm.buckets) in
    if (current_generation mod 2 == 0) && (Atomic.get hm.resize_generation == current_generation || not was_created) then (
      if (Atomic.get hm.nr_elements >= Atomic.get hm.size * 2) then (
        resize hm;
      );
      if (was_created) then Atomic.incr hm.nr_elements;
      (value, was_created)
    )
    else (
      while (Atomic.get hm.resize_generation == current_generation) do () (* spin *)
      done;
      find_create hm key)


  and resize hm =
    let current_generation = Atomic.get hm.resize_generation in
    if ((current_generation mod 2 == 0) && Atomic.compare_and_set hm.resize_generation current_generation (current_generation+1)) then (

      let old_size = Atomic.get hm.size in
      let new_size = old_size * 2 in

      let new_buckets = Array.init new_size (fun _ -> Atomic.make None) in
      let rec rehash_bucket (bucket: Bucket.t option Atomic.t) =
        match Atomic.get bucket with
        | None -> ()
        | Some element -> 
          begin
            let value = element.value in
            let key = element.key in
            let hash = abs @@ H.hash key in
            let new_location = Array.get new_buckets (hash mod new_size) in
            let _ = match Atomic.get new_location with
              | None -> (ignore @@ Atomic.set new_location (Some (Bucket.create_with_value key value)))
              | Some new_bucket -> 
                let newer_bucket = Bucket.create_with_value_and_next key value new_bucket in
                (ignore @@ Atomic.set new_location (Some newer_bucket)) in
            rehash_bucket element.next
          end
      in
      Array.iter (fun bucket -> rehash_bucket bucket) (Atomic.get hm.buckets);

      Atomic.set hm.buckets new_buckets;
      Atomic.set hm.size new_size;
      Atomic.incr hm.resize_generation;
    )

  let to_value_seq hm =
    let bucket_seq = Array.to_seq (Atomic.get hm.buckets) in 
    let rec bucket_to_value_seq (bucket : Bucket.t option Atomic.t) = match Atomic.get bucket with
      | None -> fun () -> Seq.Nil
      | Some b -> fun () -> Seq.Cons (b.value, bucket_to_value_seq b.next) in
    Seq.flat_map bucket_to_value_seq bucket_seq

  let to_hashtbl hm =
    let ht = HM.create 10 in
    let seq = to_seq hm in
    Seq.iter (fun (k, v) -> HM.add ht k (Atomic.get v)) seq;
    ht

end
