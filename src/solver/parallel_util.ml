open Batteries
open ConstrSys
open GobConfig  


module AtomicLinkedList =
struct
  type 'a node = {
    value: 'a;
    next: 'a node option Atomic.t; (* TODO: a normal mutable field could be enough *)
  }
  type 'a t = {
    head: 'a node option Atomic.t;
    last: 'a node option Atomic.t;
  }
  let create () = { head = Atomic.make None; last = Atomic.make None }

  let rec append linked_list value =
    let new_node = { value; next = Atomic.make None } in
    
    let current_last = Atomic.get linked_list.last in
    match current_last with
    | None -> 
      let success = Atomic.compare_and_set linked_list.last None (Some new_node) in
      if not success then append linked_list value
      else Atomic.set linked_list.head (Some new_node);
    | Some last_node -> 
      let success = Atomic.compare_and_set linked_list.last current_last (Some new_node) in
      if not success then append linked_list value
      else Atomic.set last_node.next (Some new_node)
end


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
  val to_string: t -> string
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

  let rec find_create (cmap: t) (key: H.t): (D.t Atomic.t * bool) =
    let find_create_with_hash (atomic_node : t) hashval =
      match Atomic.get atomic_node with
      | None -> 
        let new_node = create_default_node key in
        let success = Atomic.compare_and_set atomic_node None (Some new_node) in
        if success then (new_node.value, true) else find_create atomic_node key
      | Some node' ->
        if hashval = node'.hashval && H.equal key node'.key then
          (node'.value, false)
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

module ParallelSolverStats (S:EqConstrSys) (HM:Hashtbl.S with type key = S.v) =
struct
  open S
  open Messages

  let stack_d = Atomic.make 0
  let nr_threads = 100
  let vars = Atomic.make 0
  (* TODO 4 is selected for quick testing, should be variable *)
  let vars_by_thread = Array.init 100 (fun _ -> Atomic.make 0) 
  (* TODO now there is a reasonanle distinction between query and get_var *)
  (* or is there? *)
  let queries = Atomic.make 0
  let queries_by_thread = Array.init 100 (fun _ -> Atomic.make 0)
  let evals = Atomic.make 0
  let evals_by_thread = Array.init 100 (fun _ -> Atomic.make 0)

  let active_threads = Atomic.make 0
  let first_thread_activation_time = Atomic.make 0.0
  let last_thread_activation_update_time = Atomic.make 0.0
  let total_thread_activation_time = Atomic.make 0.0

  let updates = Atomic.make 0
  let updates_by_thread = Array.init 100 (fun _ -> Atomic.make 0)

  let full_trace = false
  let start_c = 0
  let max_c   : int ref = ref (-1)
  let max_var : Var.t option ref = ref None

  let histo = HM.create 1024
  let increase (v:Var.t) =
    let set v c =
      if not full_trace && (c > start_c && c > !max_c && not (GobOption.exists (Var.equal v) !max_var)) then begin
        if tracing then trace "sol" "Switched tracing to %a" Var.pretty_trace v;
        max_c := c;
        max_var := Some v
      end
    in
    try let c = HM.find histo v in
      set v (c+1);
      HM.replace histo v (c+1)
    with Not_found -> begin
        set v 1;
        HM.add histo v 1
      end

  let start_event () = ()
  let stop_event () = ()

  let new_var_event thread_id x =
    Atomic.incr vars;
    Atomic.incr vars_by_thread.(thread_id);
    if tracing then trace "sol" "New %a" Var.pretty_trace x

  let get_var_event x =
    Atomic.incr queries;
    if tracing && full_trace then trace "sol" "Querying %a" Var.pretty_trace x

  let eval_rhs_event thread_id x =
    if tracing && full_trace then trace "sol" "(Re-)evaluating %a" Var.pretty_trace x;
    Atomic.incr evals;
    (* Atomic.incr evals_by_thread.(thread_id); *)
    if (get_bool "dbg.solver-progress") then (Atomic.incr stack_d; Logs.debug "%d" @@ Atomic.get stack_d)

  let update_var_event thread_id x o n =
    Atomic.incr updates;
    (* Atomic.incr updates_by_thread.(thread_id); *)
    if tracing then increase x;
    if full_trace || (not (Dom.is_bot o) && GobOption.exists (Var.equal x) !max_var) then begin
      if tracing then tracei "sol_max" "(%d) Update to %a" !max_c Var.pretty_trace x;
      if tracing then traceu "sol_max" "%a" Dom.pretty_diff (n, o)
    end

  let rec thread_starts_solve_event thread_id =
    let t = Unix.gettimeofday () in
    if (Atomic.get first_thread_activation_time) = 0.0 then 
      begin
        let success = Atomic.compare_and_set first_thread_activation_time 0.0 t in
        if (not success) then thread_starts_solve_event thread_id
        else (
          Atomic.set last_thread_activation_update_time t;
          Atomic.incr active_threads;
        )
        end
    else (
        let last_thread_activation_update_time_f = Atomic.get last_thread_activation_update_time in
        let success = Atomic.compare_and_set last_thread_activation_update_time last_thread_activation_update_time_f t in
        if (not success) then thread_starts_solve_event thread_id
        else
        begin
          let time_diff = t -. last_thread_activation_update_time_f in
          let cpu_time_since_last_update = time_diff *. (float_of_int @@ Atomic.get active_threads) in
          let current_total_thread_activation_time = Atomic.get total_thread_activation_time in
          let new_total_thread_activation_time = current_total_thread_activation_time +. cpu_time_since_last_update in
          Atomic.set total_thread_activation_time new_total_thread_activation_time;
          Atomic.incr active_threads;
          end
      )

  let rec thread_ends_solve_event thread_id =
    let t = Unix.gettimeofday () in
    let last_thread_activation_update_time_f = Atomic.get last_thread_activation_update_time in
    let success = Atomic.compare_and_set last_thread_activation_update_time last_thread_activation_update_time_f t in
    if (not success) then thread_ends_solve_event thread_id
    else
      begin
        let time_diff = t -. last_thread_activation_update_time_f in
        let cpu_time_since_last_update = time_diff *. (float_of_int @@ Atomic.get active_threads) in
        let current_total_thread_activation_time = Atomic.get total_thread_activation_time in
        let new_total_thread_activation_time = current_total_thread_activation_time +. cpu_time_since_last_update in
        Atomic.set total_thread_activation_time new_total_thread_activation_time;
        Atomic.decr active_threads;
        end
   

  (* solvers can assign this to print solver specific statistics using their data structures *)
  let print_solver_stats = ref (fun () -> ())

  (* this can be used in print_solver_stats *)
  let ncontexts = ref 0
  let print_context_stats rho =
    let histo = Hashtbl.create 13 in (* histogram: node id -> number of contexts *)
    let str k = GobPretty.sprint S.Var.pretty_trace k in (* use string as key since k may have cycles which lead to exception *)
    let is_fun k = match S.Var.node k with FunctionEntry _ -> true | _ -> false in (* only count function entries since other nodes in function will have leq number of contexts *)
    HM.iter (fun k _ -> if is_fun k then Hashtbl.modify_def 0 (str k) ((+)1) histo) rho;
    (* let max_k, n = Hashtbl.fold (fun k v (k',v') -> if v > v' then k,v else k',v') histo (Obj.magic (), 0) in *)
    (* Logs.debug "max #contexts: %d for %s" n max_k; *)
    ncontexts := Hashtbl.fold (fun _ -> (+)) histo 0;
    let topn = 5 in
    Logs.debug "Found %d contexts for %d functions. Top %d functions:" !ncontexts (Hashtbl.length histo) topn;
    Hashtbl.to_list histo
    |> List.sort (fun (_,n1) (_,n2) -> compare n2 n1)
    |> List.take topn
    |> List.iter @@ fun (k,n) -> Logs.debug "%d\tcontexts for %s" n k

  let stats_csv =
    let save_run_str = GobConfig.get_string "save_run" in
    if save_run_str <> "" then (
      let save_run = Fpath.v save_run_str in
      GobSys.mkdir_or_exists save_run;
      Fpath.(to_string (save_run / "solver_stats.csv")) |> open_out |> Option.some
    ) else None
  let write_csv xs oc = output_string oc @@ String.concat ",\t" xs ^ "\n"

  (* print generic and specific stats *)
  let print_stats _ =
    Logs.newline ();
    (* print_endline "# Generic solver stats"; *)
    Logs.info "runtime: %s" (GobSys.string_of_time ());
    (* Logs.info "vars: %d, evals: %d" (Atomic.get vars) (Atomic.get evals); *)
    Logs.info "vars: %d" (Atomic.get vars);
    (* Array.iteri (fun i v -> Logs.info "    vars (%d): %d" i (Atomic.get v)) vars_by_thread; *)
    
    Logs.info "evals: %d" (Atomic.get evals);
    (* Array.iteri (fun i v -> Logs.info "    evals (%d): %d" i (Atomic.get v)) evals_by_thread; *)

    Logs.info "updates: %d" (Atomic.get updates);
    (* Array.iteri (fun i v -> Logs.info "    updates (%d): %d" i (Atomic.get v)) updates_by_thread; *)

    let first_thread_start = Atomic.get first_thread_activation_time in
    let last_registered = Atomic.get last_thread_activation_update_time in
    let average_thread_activation_time = Atomic.get total_thread_activation_time /. (last_registered -. first_thread_start) in
    let total_time = last_registered -. first_thread_start in
    Logs.info "average nr_threads: %f" (average_thread_activation_time);
    Logs.info "Evaluations per CPU-second: %f" ((float_of_int @@ Atomic.get evals) /. average_thread_activation_time /. total_time);
    Logs.info "Updates per CPU-second: %f" ((float_of_int @@ Atomic.get updates) /. average_thread_activation_time /. total_time);

    Option.may (fun v -> ignore @@ Logs.info "max updates: %d for var %a" !max_c Var.pretty_trace v) !max_var;
    Logs.newline ();
    (* print_endline "# Solver specific stats"; *)
    !print_solver_stats ();
    Logs.newline ();
    (* Timing.print (M.get_out "timing" Legacy.stdout) "Timings:\n"; *)
    (* Gc.print_stat stdout; (* too verbose, slow and words instead of MB *) *)
    let gc = GobGc.print_quick_stat Legacy.stderr in
    Logs.newline ();
    Option.may (write_csv [GobSys.string_of_time (); string_of_int !SolverStats.vars; string_of_int !SolverStats.evals; string_of_int !ncontexts; string_of_int gc.Gc.top_heap_words]) stats_csv
  (* print_string "Do you want to continue? [Y/n]"; *)
  (* flush stdout *)
  (* if read_line () = "n" then raise Break *)

  let () =
    let write_header = write_csv ["runtime"; "vars"; "evals"; "contexts"; "max_heap"] (* TODO @ !solver_stats_headers *) in
    Option.may write_header stats_csv;
    (* call print_stats on dbg.solver-signal *)
    Sys.set_signal (GobSys.signal_of_string (get_string "dbg.solver-signal")) (Signal_handle print_stats);
    (* call print_stats every dbg.solver-stats-interval *)
    Sys.set_signal Sys.sigvtalrm (Signal_handle print_stats);
    (* https://ocaml.org/api/Unix.html#TYPEinterval_timer ITIMER_VIRTUAL is user time; sends sigvtalarm; ITIMER_PROF/sigprof is already used in Timeout.Unix.timeout *)
    let ssi = get_int "dbg.solver-stats-interval" in
    if ssi > 0 then
      let it = float_of_int ssi in
      ignore Unix.(setitimer ITIMER_VIRTUAL { it_interval = it; it_value = it });
end
