(* @see <https://doi.org/10.1017/S0960129521000499> Seidl, H., Vogler, R. Three improvements to the top-down solver. *)
(* @see <https://arxiv.org/abs/2209.10445> Interactive Abstract Interpretation: Reanalyzing Whole Programs for Cheap. *)

(** Terminating top down solver that is parallelized for some cases, where multiple unknowns have to be solved for a rhs. *)
(* TD3: see paper 'Three Improvements to the Top-Down Solver' https://dl.acm.org/doi/10.1145/3236950.3236967
 * Option solvers.td3.* (default) ? true : false (solver in paper):
 * - term (true) ? use phases for widen+narrow (TDside) : use box (TDwarrow)*)

open Batteries
open ConstrSys
open Messages

open Parallel_util

(* parameters - TODO: change to goblint options *)
let lowest_prio = 10
let highest_prio = 0
let map_size = 1000

module M = Messages

module Base : GenericEqSolver =
functor (S:EqConstrSys) ->
functor (HM:Hashtbl.S with type key = S.v) ->
struct
  open SolverBox.Warrow (S.Dom)
  include Generic.SolverStats (S) (HM)
  module VS = Set.Make (S.Var)
  module LHM = LockableHashtbl (S.Var) (HM)
  module Int_tbl = Hashtbl.Make (
  struct
    type t = int
    let equal = (=)
    let hash = Hashtbl.hash
  end
  )
  module PLHM = LockableHashtbl (
  struct
    type t = int
    let equal = (=)
    let hash = Hashtbl.hash
  end
  ) (Int_tbl)

  type var_data = {
    rho: S.Dom.t;
    called: int;
    stable: int;
  }

  (* data of *)
  type solver_data = var_data LHM.t

  type thread_data = {
    wpoint: unit HM.t;
    infl: VS.t HM.t;
  } 


  let create_empty_solver_data () = LHM.create map_size

  let create_empty_thread_data () = 
    {
      wpoint = HM.create 10;
      infl = HM.create 10;
    }

  let print_data data =
    Logs.debug "|vars|=%d" (LHM.length data)

  let print_data_verbose data str =
    if Logs.Level.should_log Debug then (
      Logs.debug "%s:" str;
      print_data data
    )

  let print_iteration_counts counter =
    PLHM.iter (fun k v -> Logs.info "Thread %d iterations: %d" k v) counter

  let solve st vs =

    let nr_threads = GobConfig.get_int "solvers.td3.parallel_domains" in
    let lower_in_search_phase = ref true in

    let data = create_empty_solver_data ()
    in

    let iterate_counter = PLHM.create 10 in

    let main_finished = ref false in
    (* Search time must be measured manually, as Timing.wrap does not seem to support parallelism. *)
    let search_time = ref 0.0 in 
    let nr_restarts = Atomic.make 0 in
    (* let main_finished_mutex = Mutex.create () in *)
    let nr_search = Atomic.make 0 in
    let nr_work = Atomic.make 0 in

    let () = print_solver_stats := fun () ->
      Logs.info "Lower in search phase: %b" !lower_in_search_phase;
      print_iteration_counts iterate_counter;
      Logs.info "Search time: %f" !search_time;
      Logs.info "Nr restarts: %d" (Atomic.get nr_restarts);
      Logs.info "Search percentage: %f" ((float_of_int (Atomic.get nr_search)) /. (float_of_int ((Atomic.get nr_search) + (Atomic.get nr_work))));
      print_data data;
      (* TODO adapt next line to new data structure *)
      (* print_context_stats @@ LHM.to_hashtbl rho *)
    in

    let init x =
      (* Init must be called while holding x lock *)
      if not (LHM.mem data x) then (
        new_var_event x;
        if tracing then trace "init" "initializing %a(%d)" S.Var.pretty_trace x (S.Var.hash x);
        LHM.replace data x ({
          rho=S.Dom.bot ();
          stable=lowest_prio;
          called=lowest_prio
        })
      )
    in

    (* returns the current called/stable prio value of x. returns lowest_prio if not present *)
    (* TODO consider adding a joint version of this to spare locks *)
    let stable_prio x = match LHM.find_option data x with
      | Some vd -> vd.stable
      | None -> lowest_prio in

    let called_prio x = match LHM.find_option data x with
      | Some vd -> vd.called
      | None -> lowest_prio in

    let eq x (get : S.v -> S.d) set =
      match S.system x with
      | None -> S.Dom.bot ()
      | Some f -> f get set
    in

    let rec solve_thread x thread_id =
      let () = if (PLHM.mem iterate_counter thread_id) then () else (PLHM.replace iterate_counter thread_id 0) in
      
      let prio = (lowest_prio - thread_id - 1) in


      let rec find_work (worklist: S.v list) (seen: VS.t) = 
        match worklist, thread_id with
        | [], _ -> 
          if tracing then trace "work" "%d found no work" prio;
          None
        | x :: xs, 9 -> Some x
        | x :: xs, _ ->
          if tracing then trace "search" "%d searching for work %a" prio S.Var.pretty_trace x;
          let maybe_eq = S.system x in
          LHM.lock x data;
          let workable = prio < called_prio x && prio < stable_prio x && Option.is_some maybe_eq in
          LHM.unlock x data;
          if (workable) then (
            if tracing then trace "work" "%d found work %a" prio S.Var.pretty_trace x;
            Some x
          )
          else (
            let new_work = ref [] in
            let query (y: S.v): S.d =
              if (not (VS.mem y seen)) then new_work := y :: !new_work;
              LHM.lock y data;
              let result = match LHM.find_option data y with
                | Some vd -> vd.rho
                | None -> S.Dom.bot () in
              LHM.unlock y data;
              result
            in
            let side _ _ = () in
            ignore @@ Option.map (fun eq -> eq query side) maybe_eq; 
            (* eq x query side; *)
            if !main_finished then None else (
              (* Introduce some randomness to prevent getting caught in unproductive sectors *)
              let next_work = if (Random.bool ()) then (xs @ !new_work) else (!new_work @ xs) in
              (* let next_work = xs @ !new_work in *)
              find_work next_work (VS.add_seq (Seq.of_list !new_work) seen))
          )
      in

      let do_work x =
        (* init thread local data *)
        let t_data = create_empty_thread_data () in
        let wpoint = t_data.wpoint in
        let infl = t_data.infl in


        let add_infl y x =
          if tracing then trace "infl" "%d add_infl %a %a" prio S.Var.pretty_trace y S.Var.pretty_trace x;
          HM.replace infl y (VS.add x (HM.find_default infl y VS.empty));
        in

        let rec destabilize ~all x =
          let w = HM.find_default infl x VS.empty in
          HM.replace infl x VS.empty;
          VS.iter (fun y ->
            if tracing then trace "lock" "%d locking %a in destab" prio S.Var.pretty_trace y;
            LHM.lock y data;
            if (all || (prio <= stable_prio y)) && (stable_prio y <> lowest_prio) then (
              if tracing then trace "destab" "%d destabilizing %a from %d" prio S.Var.pretty_trace y (stable_prio y);
              (* TODO this can be improved with Atomic *)
              let old_value = LHM.find data y in
              LHM.replace data y {old_value with stable=lowest_prio};
            );
            if tracing then trace "lock" "%d unlocking %a in destab" prio S.Var.pretty_trace y;
            LHM.unlock y data;
            destabilize ~all y
          ) w
        in

        let rec iterate (x : S.v) = (* ~(inner) solve in td3*)

          let query x y = (* ~eval in td3 *)
            if tracing then trace "sol_query" "%d entering query with prio %d for %a" prio (called_prio y) S.Var.pretty_trace y;
            if tracing then trace "lock" "%d locking %a in query" prio S.Var.pretty_trace y;
            LHM.lock y data;
            if tracing then trace "lock" "%d locked %a in query" prio S.Var.pretty_trace y;
            get_var_event y;
            if (prio < called_prio y) then ( (* Priority high enough: Take over the variable and iterate *)

              (* TODO (see td-parallel repo): check if necessary/enough *)
              (* If owning new, make sure it is not in point *)
              if tracing && (HM.mem wpoint y) then trace "wpoint" "%d query removing wpoint %a" prio S.Var.pretty_trace y;
              if HM.mem wpoint y then HM.remove wpoint y;
              init y;
              if S.system y = None then (
                if tracing then trace "stable" "%d query setting %a stable from %d" prio S.Var.pretty_trace y (stable_prio y);
                (* TODO this can be improved with Atomic *)
                let old = LHM.find data y in
                LHM.replace data y {old with stable=prio};
              ) else (
                if tracing then trace "called" "%d query setting prio from %d to %d for %a" prio (called_prio y) prio S.Var.pretty_trace y;
                if tracing then trace "own" "%d taking ownership of %a." prio S.Var.pretty_trace y;
                (* if tracing && ((called_prio y) != lowest_prio) then trace "steal" "%d stealing %a from %d" prio S.Var.pretty_trace y (called_prio y); *)
                if (called_prio y != lowest_prio) then (
                  if tracing then trace "steal" "steal from: %d %a" (called_prio y) S.Var.pretty_trace y;
                );
                (* TODO this can be improved with Atomic *)
                let old = LHM.find data y in
                LHM.replace data y {old with called=prio};
                if tracing then trace "lock" "%d unlocking %a in query" prio S.Var.pretty_trace y;
                LHM.unlock y data;
                (* call iterate unlocked *)
                if tracing then trace "iter" "%d iterate called from query" prio;
                iterate y;
                if tracing then trace "lock" "%d locking %a in query 2" prio S.Var.pretty_trace y;
                LHM.lock y data;
                if (called_prio y >= prio) then ( (* if still owning *)
                  if tracing then trace "own" "%d giving up ownership of %a." prio S.Var.pretty_trace y;
                  if tracing then trace "called" "%d query setting prio back from %d to %d for %a" prio (called_prio y) lowest_prio S.Var.pretty_trace y;
                  (* TODO this can be improved with Atomic *)
                  let old = LHM.find data y in
                  LHM.replace data y {old with called=lowest_prio};
                )
              )
            ) else (
              if tracing && not (HM.mem wpoint y) then trace "wpoint" "%d query adding wpoint %a from %a" prio S.Var.pretty_trace y S.Var.pretty_trace x;
              HM.replace wpoint y () (* is this correct, we could also just be skipping due to low prio *)
              (* since wpoint is thread local it should be fine either way *)
            );
            let result = (LHM.find data y).rho in
            if tracing then trace "lock" "%d unlocking %a in query 2" prio S.Var.pretty_trace y;
            LHM.unlock y data;
            add_infl y x;
            if tracing then trace "sol_query" "%d exiting query for %a" prio S.Var.pretty_trace y;
            result
          in

          let side x y d = (* side from x to y; only to variables y w/o rhs; x only used for trace *)
            if tracing then trace "side" "%d side to %a(%d) (wpx: %b) from %a ## value: %a" prio S.Var.pretty_trace y (S.Var.hash y) (HM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty d;
            if S.system y <> None then (
              Logs.warn "side-effect to unknown w/ rhs: %a, contrib: %a" S.Var.pretty_trace y S.Dom.pretty d;
            );
            assert (S.system y = None);
            if tracing then trace "lock" "%d locking %a in side" prio S.Var.pretty_trace y;
            LHM.lock y data;
            init y;

            (* begining of side *)
            (* TODO check if this works *)
            if (called_prio y >= prio) then (
              if tracing then trace "called" "%d side setting prio from %d to %d for %a" prio (called_prio y) prio S.Var.pretty_trace y;
              if tracing then trace "ownSide" "%d side taking ownership of %a. Previously owned by %d" prio S.Var.pretty_trace y (called_prio y);
              let oldr = LHM.find data y in
              LHM.replace data y {oldr with called=prio};
              let old = oldr.rho in
              (* currently any side-effect after the first one will be widened *)
              let widen a b = 
                if M.tracing then M.trace "sidew" "%d side widen %a" prio S.Var.pretty_trace y;
                S.Dom.widen a (S.Dom.join a b)    
              in 
              let tmp = if HM.mem wpoint y then widen old d else S.Dom.join old d in
              if not (S.Dom.leq tmp old) then (
                if tracing then trace "updateSide" "%d side setting %a(%d) to %a" prio S.Var.pretty_trace y (S.Var.hash y) S.Dom.pretty tmp;
                let oldr = LHM.find data y in
                LHM.replace data y {oldr with rho=tmp};
                if tracing then trace "lock" "%d unlocking %a in side" prio S.Var.pretty_trace y;
                LHM.unlock y data;
                if tracing then trace "destab" "%d destabilize called from side to %a" prio S.Var.pretty_trace y;
                destabilize ~all:true y;
                if tracing && not (HM.mem wpoint y) then trace "wpoint" "%d side adding wpoint %a from %a" prio S.Var.pretty_trace y S.Var.pretty_trace x;
                HM.replace wpoint y ()
              ) else (
                if tracing then trace "lock" "%d unlocking %a in side" prio S.Var.pretty_trace y;
                LHM.unlock y data
              )
            ) else (
              if tracing then trace "lock" "%d unlocking %a in side" prio S.Var.pretty_trace y;
              LHM.unlock y data
            )
          in


          (* begining of iterate *)
          let () = PLHM.replace iterate_counter thread_id (PLHM.find iterate_counter thread_id + 1) in
          if tracing then trace "prio" "%d %d" prio (PLHM.find iterate_counter thread_id);
          if tracing then trace "lock" "%d locking %a in iterate" prio S.Var.pretty_trace x;
          LHM.lock x data;
          init x;
          if tracing then trace "iter" "%d iterate %a, called: %b, stable: %b, wpoint: %b" prio S.Var.pretty_trace x (called_prio x <= prio) (stable_prio x <= prio) (HM.mem wpoint x);
          assert (S.system x <> None);
          if not (stable_prio x <= prio) then (
            if tracing then trace "stable" "%d iterate setting %a stable from %d" prio S.Var.pretty_trace x (stable_prio x);
            let old = LHM.find data x in
            LHM.replace data x {old with stable=prio};
            (* Here we cache LHM.mem wpoint x before eq. If during eq evaluation makes x wpoint, then be still don't apply widening the first time, but just overwrite.
                 It means that the first iteration at wpoint is still precise.
                 This doesn't matter during normal solving (?), because old would be bot.
                 This matters during incremental loading, when wpoints have been removed (or not marshaled) and are redetected.
                 Then the previous local wpoint value is discarded automagically and not joined/widened, providing limited restarting of local wpoints. (See query for more complete restarting.) *)
            let wp = HM.mem wpoint x in (* if x becomes a wpoint during eq, checking this will delay widening until next iterate *)
            if tracing then trace "lock" "%d unlocking %a in iterate" prio S.Var.pretty_trace x;
            LHM.unlock x data;
            if tracing then trace "eq" "%d eval eq for %a" prio S.Var.pretty_trace x;
            let eqd = eq x (query x) (side x) in
            if tracing then trace "lock" "%d locking %a in iterate 2" prio S.Var.pretty_trace x;
            LHM.lock x data;
            let old = (LHM.find data x).rho in (* d from older iterate *) (* find old value after eq since wpoint restarting in eq/query might have changed it meanwhile *)
            let wpd = (* d after box operator (if wp) *)
              if not wp then 
                eqd
              else (if tracing then trace "wpoint" "%d box widening %a" prio S.Var.pretty_trace x; box old eqd)
            in
            if tracing then trace "sol" "%d Var: %a (wp: %b)\nOld value: %a\nEqd: %a\nNew value: %a" prio S.Var.pretty_trace x wp S.Dom.pretty old S.Dom.pretty eqd S.Dom.pretty wpd;
            if not (Timing.wrap "S.Dom.equal" (fun () -> S.Dom.equal old wpd) ()) then ( 
              (* old != wpd *)
              if (stable_prio x >= prio && called_prio x >= prio) then (
                if tracing then trace "sol" "%d Changed" prio;
                update_var_event x old wpd;
                if tracing then trace "update" "%d setting %a to %a" prio S.Var.pretty_trace x S.Dom.pretty wpd;
                let oldr = LHM.find data x in
                LHM.replace data x {oldr with rho=wpd};
                if tracing then trace "lock" "%d unlocking %a in iterate 2" prio S.Var.pretty_trace x;
                LHM.unlock x data;
                if tracing then trace "destab" "%d destabilize called from iterate of %a" prio S.Var.pretty_trace x;
                destabilize ~all:false x;
                if tracing then trace "iter" "%d iterate changed" prio;
                (*   (* STEAL REVIVAL HERE *) *)
                (*   if (LHM.mem stolen x && HM.mem wpoint x && revival_on ) then ( *)
                (*     if tracing then trace "steal" "%d revived at %a" (prio+1) S.Var.pretty_trace x; *)
                (*     LHM.remove stolen x; *)
                (*     (* TODO sparing locks for now - two threads, other is dead *) *)
                (*     LHM.replace called x (prio + 1); *)
                (*     (* TODO sleep here is just experimental *) *)
                (*     ignore @@ Domain.spawn (fun () -> solve_thread x (thread_id-1)); *)
                (*     Unix.sleepf 0.01; *)
                (* ); *)

                (iterate[@tailcall]) x
              ) else (
                if tracing then trace "lock" "%d unlocking %a in iterate 2" prio S.Var.pretty_trace x;
                LHM.unlock x data
              )
            ) else (
              (* old = wpd*)
              if not (stable_prio x <= prio) then (
                if tracing then trace "iter" "%d iterate still unstable" prio;
                if tracing then trace "lock" "%d unlocking %a in iterate 2" prio S.Var.pretty_trace x;
                LHM.unlock x data;
                (iterate[@tailcall]) x
              ) else (
                if tracing && (HM.mem wpoint x) then trace "wpoint" "%d iterate removing wpoint %a" prio S.Var.pretty_trace x;
                HM.remove wpoint x;
                if tracing then trace "lock" "%d unlocking %a in iterate 2" prio S.Var.pretty_trace x;
                LHM.unlock x data
              )
            )
          ) else (
            if tracing then trace "lock" "%d unlocking %a in iterate" prio S.Var.pretty_trace x;
            LHM.unlock x data;
          )
        in
        iterate x in

      let highest_thread_id = 9 in

      if (thread_id < highest_thread_id) then ( 
        lower_in_search_phase := true;
      );
      (* let start_time = Unix.gettimeofday () in *)
      (* Logs.info "Thread %d finding work" thread_id; *)
      let to_iterate = find_work [ x ] VS.empty in
      (* Logs.info "Thread %d found work" thread_id; *)
      (* search_time := !search_time +. (Unix.gettimeofday () -. start_time); *)
      if (thread_id < highest_thread_id) then ( 
        lower_in_search_phase := false;
      );
      begin match to_iterate with
        | Some x -> begin 
          if tracing then trace "start" "Thread %d started at %a" thread_id S.Var.pretty_trace x;
          if tracing then trace "lock" "%d locking %a in start_threads" prio S.Var.pretty_trace x;
          LHM.lock x data; (* We should bundle data for variables again, so that 
                             lock locks the complete variable (even if we consistently
                             use rho, lock is also required for LHM to function properly) *)
          if tracing then trace "lock" "%d locked %a in start_threads" prio S.Var.pretty_trace x;
          let old = match (LHM.find_option data x) with
            | Some vd -> vd
            | None -> {rho=S.Dom.bot (); called=lowest_prio; stable=lowest_prio} in
          if tracing then trace "lock" "%d found old %a in start_threads" prio S.Var.pretty_trace x;
          (* // TODO this should be safe *)
          LHM.replace data x {old with called=prio};
          if tracing then trace "start" "Thread %d really started at %a" thread_id S.Var.pretty_trace x;
          if tracing then trace "lock" "%d unlocking %a in start_threads" prio S.Var.pretty_trace x;
          LHM.unlock x data;
          (* Prevent lower threads from doing work to measure the overhead from find_work *)
          if (thread_id = 9) then do_work x
          end
        | None -> () end;
      if (thread_id < highest_thread_id) then ( 
        if tracing then trace "finish" "Thread %d finished" thread_id;
        (* Unix.sleepf 0.01; *)
        if (not !main_finished) then (
          Atomic.incr nr_restarts;
          (* Unix.sleepf 0.05; *)
          (* while (not !main_finished) do (); done; *)
          (solve_thread[@tailcall]) x thread_id
        )
      )
      else (
        if tracing then trace "finish" "Thread %d finished" thread_id;
        main_finished := true;
        Logs.info "Main done";
      )
    in

    let set_start (x,d) =
      init x;
      let oldr = LHM.find data x in
      LHM.replace data x {oldr with rho=d; stable=highest_prio};
    (* iterate x Widen *)
    in

    let start_threads x =
      (* let threads = Array.init nr_threads (fun j -> *)
      (*   Domain.spawn (fun () ->  *)
      (*     if Logs.Level.should_log Debug then Printexc.record_backtrace true; *)
      (*     Unix.sleepf (Float.mul (float j) 0.01); *)
      (*     try *)
      (*       if tracing then trace "start" "%d started" j; *)
      (*       solve_thread x j *)
      (*     with  *)
      (*       e -> Printexc.print_backtrace stderr; *)
      (*       raise e *)
      (*   )) in  *)
      (* Array.iter Domain.join threads; *)
      (* let main_solver_thread = Domain.spawn (fun () -> solve_thread x 2) in *)
      (* let lower_solver_thread = Domain.spawn (fun () -> solve_thread x 0) in *)
      let poll () = while (not !main_finished) do
        if (!lower_in_search_phase) then (Atomic.incr nr_search) else (Atomic.incr nr_work)
      done in
      let solver_start_time = Unix.gettimeofday () in
      let threads = Array.init nr_threads (fun j -> 
        Domain.spawn (fun () ->
          (* Processor.Affinity.set_cpus (Processor.Cpu.from_core (8+j) Processor.Topology.t); *)
          solve_thread x (9-j))
      ) in
      let poll_thread = Domain.spawn (fun () -> poll ()) in
      (* Processor.Affinity.set_cpus (Processor.Cpu.from_core 0 Processor.Topology.t); *)
      Array.iter Domain.join threads;
      Domain.join poll_thread;
      if tracing then (PLHM.iter (fun k v -> trace "cpri" "Thread %d iterated %d times" k v) iterate_counter);
      if tracing then trace "stime" "Nr restarts %d" (Atomic.get nr_restarts);
      !print_solver_stats ();
      Logs.info "Solver time: %f" (Unix.gettimeofday () -. solver_start_time);
    in

    (* Imperative part starts here*)
    start_event ();

    List.iter set_start st;

    List.iter init vs;
    (* If we have multiple start variables vs, we might solve v1, then while solving v2 we side some global which v1 depends on with a new value. Then v1 is no longer stable and we have to solve it again. *)
    let i = ref 0 in
    let rec solver () = (* as while loop in paper *)
      incr i;
      let unstable_vs = List.filter (neg (fun x -> (match LHM.find_option data x with 
        | Some vd -> vd.stable
        | None -> lowest_prio) < lowest_prio)) vs in
      if unstable_vs <> [] then (
        if Logs.Level.should_log Debug then (
          if !i = 1 then Logs.newline ();
          Logs.debug "Unstable solver start vars in %d. phase:" !i;
          List.iter (fun v -> Logs.debug "\t%a" S.Var.pretty_trace v) unstable_vs;
          Logs.newline ();
          flush_all ();
        );
        List.iter (fun x -> (*LHM.replace called x highest_prio;*)
          if tracing then trace "multivar" "solving for %a" S.Var.pretty_trace x;
          Timing.wrap "start_threads" start_threads x;
          if tracing then (
            trace "multivar" "current mapping:";
            (* TODO fix if possible and relevant *)
            (* LHM.iter (fun k v -> trace "multivar" "%a (%d) -----> %a" S.Var.pretty_trace k (S.Var.hash k) S.Dom.pretty v) rho *)
          )
          (*LHM.replace called x lowest_prio*)) unstable_vs;
        solver ();
      )
    in
    solver ();
    (* Before we solved all unstable vars in rho with a rhs in a loop. This is unneeded overhead since it also solved unreachable vars (reachability only removes those from rho further down). *)
    (* After termination, only those variables are stable which are
       * - reachable from any of the queried variables vs, or
       * - effected by side-effects and have no constraints on their own (this should be the case for all of our analyses). *)

    stop_event ();
    print_data_verbose data "Data after iterate completed";

    (* TODO: somehow print wpoints*)
    (*if GobConfig.get_bool "dbg.print_wpoints" then (
        Logs.newline ();
        Logs.debug "Widening points:";
        HM.iter (fun k () -> Logs.debug "%a" S.Var.pretty_trace k) wpoint;
        Logs.newline ();
        );*)


    print_data_verbose data "Data after postsolve";

    HM.map (fun k v -> v.rho) (LHM.to_hashtbl data) 
end

let () =
  Selector.add_solver ("td_parallel_stealing_cmap", (module PostSolver.EqIncrSolverFromEqSolver (Base)));
