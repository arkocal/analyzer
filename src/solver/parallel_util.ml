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
    Logs.info "LHM info: Buckets = %d    Max = %d   Mean = %f    SD = %f" (Array.length lhm) (max_bucket lhm) (mean_bucket lhm) (sd_bucket lhm);
    Logs.newline ();
    flush_all ()
end 

module Thread_pool =
struct
  module T = Domainslib.Task

  let create n = T.setup_pool ~num_domains:n ()

  let add_work pool f = T.async pool f 

  let await_all pool promises = 
    List.iter (T.await pool)  promises

  let finished_with pool = T.teardown_pool pool
end