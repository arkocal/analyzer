open Batteries

module LockableHashtbl (H:Hashtbl.HashedType) (HM:Hashtbl.S with type key = H.t) = 
struct
  type key = HM.key
  type 'a t = (GobMutex.t * 'a HM.t) array

  let bucket_index lhm k = Int.abs ((H.hash k) mod (Array.length lhm))

  let create sz = Array.init sz (fun _ -> GobMutex.create (), HM.create 10)

  let length lhm = Array.fold (fun l (_, hm) -> l + (HM.length hm)) 0 lhm

  let find lhm k = 
    let i = bucket_index lhm k in
    let (_, hm) = Array.get lhm i in
    HM.find hm k

  let find_default lhm k d =
    let i = bucket_index lhm k in
    let (_, hm) = Array.get lhm i in
    HM.find_default hm k d

  let remove lhm k = 
    let i = bucket_index lhm k in
    let (_, hm) as ele = Array.get lhm i in
    HM.remove hm k

  let replace lhm k v =
    let i = bucket_index lhm k in
    let (_, hm) as ele = Array.get lhm i in
    HM.replace hm k v

  let mem lhm k = 
    let i = bucket_index lhm k in
    let (_, hm) = Array.get lhm i in
    HM.mem hm k

  let to_hashtbl lhm = 
    let mapping_list = Array.fold (fun acc (_, hm) -> List.append acc (HM.to_list hm)) [] lhm in
    HM.of_list mapping_list

  let lock k lhm = 
    let (me, _) = Array.get lhm (bucket_index lhm k) in
    GobMutex.lock me

  let unlock k lhm = 
    let (me, _) = Array.get lhm (bucket_index lhm k) in
    GobMutex.unlock me

  (* debug functions *)
  let max_bucket lhm = Array.fold (fun max (_, hm) -> Int.max max (HM.length hm)) 0 lhm

  let mean_bucket lhm = (float @@ length lhm) /. (float @@ Array.length lhm)

  let sd_bucket lhm = 
    let mean = mean_bucket lhm in
    let sq_dev = Array.map (fun (_, hm) -> Float.pow ((float @@ HM.length hm) -. mean) 2.) lhm in
    let dev_sum = Array.fold Float.add 0. sq_dev in
    Float.root (dev_sum /. (float @@ Array.length lhm)) 2

  let print_stats lhm =
    Logs.newline ();
    let empty = Array.count_matching (fun (_, hm) ->  HM.is_empty hm) lhm in
    Logs.info "LHM info: Buckets = %d    Empty = %d   Max = %d   Mean = %f    SD = %f" (Array.length lhm) empty (max_bucket lhm) (mean_bucket lhm) (sd_bucket lhm);
    Logs.newline ();
    flush_all ()
end 

module Thread_pool =
struct
  module T = Domainslib.Task

  let create n = T.setup_pool ~num_domains:n ()

  let add_work pool f = T.async pool f 

  let await_all pool promises = 
    List.iter (T.await pool) promises

  let finished_with pool = T.teardown_pool pool
end


module type DefaultType = sig
  type t
  val default: unit -> t
end

module NormalHMWrapper (H:Hashtbl.HashedType) (D: DefaultType) (HM:Hashtbl.S with type key = H.t) =
  struct

  type t = D.t Atomic.t HM.t
  let create () = HM.create 10

  let find_option (hm : t) (key : H.t): D.t Atomic.t option =
    try Some (HM.find hm key)
    with Not_found -> None
  
  let mem = HM.mem

  let find (hm : t) (key : H.t): D.t Atomic.t =
    HM.find hm key

  let to_list (hm: t): (H.t * D.t Atomic.t) list =
    HM.to_list hm

  let to_hashtbl (hm: t): D.t HM.t =
    HM.of_list @@ List.map (fun (k, v) -> (k, Atomic.get v)) @@ to_list hm

  let find_create (hm: t) (key: H.t): D.t Atomic.t =
    try HM.find hm key
    with Not_found -> 
      let new_value = Atomic.make (D.default ()) in
      HM.add hm key new_value;
      new_value
end

module CreateOnlyConcurrentMap (H:Hashtbl.HashedType) (D: DefaultType) (HM:Hashtbl.S with type key = H.t) =
  struct
  type node = {
    left: (node option) Atomic.t;
    right: (node option) Atomic.t;
    key: H.t;
    hashval: int;
    value: D.t Atomic.t;
  }

  type t = (node option) Atomic.t

  let create () = Atomic.make None 

  let create_default_node key = 
    let new_node =
      {  
        left = Atomic.make None;
        right = Atomic.make None;
        key = key;
        hashval = H.hash key;
        value = Atomic.make (D.default ());
      }
    in new_node

  let find_option (cmap : t) (key : H.t): D.t Atomic.t option =
    let rec find_option_with_hash (search_node : node option) (hashval : int) =
      match search_node with
      | None -> None
      | Some node' ->
        if hashval = node'.hashval && H.equal key node'.key then
          Some node'.value
        else if hashval <= node'.hashval then
          find_option_with_hash (Atomic.get node'.left) hashval 
        else
          find_option_with_hash (Atomic.get node'.right) hashval
    in
    find_option_with_hash (Atomic.get cmap) (H.hash key)
  
  let mem (cmap : t) (key : H.t): bool =
    Option.is_some @@ find_option cmap key
  
  let find (cmap : t) (key : H.t): D.t Atomic.t =
    match find_option cmap key with
    | None -> Logs.error "Not found here"; raise Not_found
    | Some value -> value    

  let to_list (cmap: t): (H.t * D.t Atomic.t) list =
    let rec to_list' node acc =
      match node with
      | None -> acc
      | Some node' ->
        let acc = to_list' (Atomic.get node'.left) acc in
        let acc = (node'.key, node'.value) :: acc in
        to_list' (Atomic.get node'.right) acc
    in
    to_list' (Atomic.get cmap) []

  let to_hashtbl (cmap: t): D.t HM.t =
    HM.of_list @@ List.map (fun (k, v) -> (k, Atomic.get v)) @@ to_list cmap

  let rec find_create (cmap: t) (key: H.t): D.t Atomic.t =
    let find_create_with_hash (atomic_node : t) hashval =
      match Atomic.get atomic_node with
      | None -> 
        let new_node = create_default_node key in
        let success = Atomic.compare_and_set atomic_node None (Some new_node) in
        if success then new_node.value else find_create atomic_node key
      | Some node' ->
        if hashval = node'.hashval && H.equal key node'.key then
          node'.value
        else if hashval <= node'.hashval then
          find_create node'.left key
        else
          find_create node'.right key
    in find_create_with_hash cmap (H.hash key)

  (* let find_default_creating (key : H.t) node = *)
  (*   let rec find_default_creating_with_hash hashval node = *)
  (*     match node with *)
  (*     | None -> assert false *)
  (*     | Some node' -> *)
  (*       if hashval = node'.hashval && Option.map_default (H.equal key) false node'.key then ( *)
  (*         node') *)
  (*       else if hashval <= node'.hashval then *)
  (*         (match Atomic.get node'.left with *)
  (*           | None -> *)
  (*             Logs.info "Creating left node for %d" hashval; *)
  (*             let success = Atomic.compare_and_set node'.left None (Some (create_default_node @@ Some key)) in *)
  (*             assert success; *)
  (*             find_default_creating_with_hash hashval @@ Atomic.get node'.left *)
  (*           | Some left -> find_default_creating_with_hash hashval (Some left) *)
  (*         ) *)
  (*       else ( *)
  (*         match Atomic.get node'.right with *)
  (*         | None -> *)
  (*           Logs.info "Creating right node for %d" hashval; *)
  (*           let success = Atomic.compare_and_set node'.right None (Some (create_default_node @@ Some key)) in *)
  (*           assert success; *)
  (*           Logs.info "Created right node for %d" hashval; *)
  (*           find_default_creating_with_hash hashval @@ Atomic.get node'.right *)
  (*         | Some right -> find_default_creating_with_hash hashval (Some right)  *)
  (*       ) in *)
  (*   let res_node = (find_default_creating_with_hash (H.hash key) node) in *)
  (*   match res_node.key with *)
  (*   | None -> assert false *)
  (*   | Some nodekey -> ( *)
  (*     Logs.info "Found here again some key with hash %d" (H.hash nodekey); *)
  (*     assert (H.equal nodekey key); *)
  (*     Logs.info "Assert equal success"; *)
  (*   ); *)
  (*   res_node.value *)

end
