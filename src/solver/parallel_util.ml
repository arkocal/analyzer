open Batteries

module LockableHashtbl (H:Hashtbl.HashedType) (HM:Hashtbl.S with type key = H.t) = 
struct
  type key = HM.key
  type 'a t = (Dmutex.t * 'a HM.t) array

  (* double hash to make sure that not all elements in a top-level bucket have the same hash*)
  let bucket_index lhm k = Int.abs ((Hashtbl.hash (H.hash k)) mod (Array.length lhm))

  let create sz = Array.init sz (fun _ -> Dmutex.create (), HM.create 10)

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
    HM.remove hm k;
    Array.set lhm i ele

  let replace lhm k v =
    let i = bucket_index lhm k in
    let (_, hm) as ele = Array.get lhm i in
    HM.replace hm k v;
    Array.set lhm i ele

  let mem lhm k = 
    let i = bucket_index lhm k in
    let (_, hm) = Array.get lhm i in
    HM.mem hm k

  let iter f lhm = Array.iter (fun (_, hm) -> HM.iter f hm) lhm
  let iter_safe f lhm = Array.iter (fun (me, hm) -> Dmutex.lock me; HM.iter f hm; Dmutex.unlock me) lhm

  let to_hashtbl lhm = 
    let mapping_list = Array.fold (fun acc (_, hm) -> List.append acc (HM.to_list hm)) [] lhm in
    HM.of_list mapping_list

  let lock k lhm = 
    let (me, _) = Array.get lhm (bucket_index lhm k) in
    Dmutex.lock me

  let unlock k lhm = 
    let (me, _) = Array.get lhm (bucket_index lhm k) in
    Dmutex.unlock me
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