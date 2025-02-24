(** Terminating, parallelized top-down solver with side effects. ([td_parallel_base]). TODO: better Name*)

(** Top down solver that is parallelized. TODO: better description *)
(* Options:
 * - solvers.td3.parallel_domains (default: 0 - automatic selection): Maximal number of Domains that the solver can use in parallel.
 * TODO: support 'solvers.td3.remove-wpoint' option? currently it acts as if this option was always enabled *)

open Batteries
open ConstrSys
open GobConfig
open Goblint_parallel
open ParallelStats
open Messages


module M = Messages

module CasStat = struct
  let count_success = Atomic.make 0
  let count_failure = Atomic.make 0
  let cas key old new_ =
    if Atomic.compare_and_set key old new_ then
      (Atomic.incr count_success; true)
    else
      (Atomic.incr count_failure; false)
end
(* module DefaultInt = struct *)
(*   type t = int *)
(*   let default () = 0 *)
(* end *)

module Base : GenericCreatingEqSolver =
  functor (S:CreatingEqConstrSys) ->
  functor (HM:Hashtbl.S with type key = S.v) ->
  struct
    open SolverBox.Warrow (S.Dom)
    (* TODO: Maybe different solver stats according for CreatingEQsys is needed *)
    (* include Generic.SolverStats (EqConstrSysFromCreatingEqConstrSys (S)) (HM) *)
    (* include ParallelSolverStats (EqConstrSysFromCreatingEqConstrSys (S)) (HM) *)
    module VS = Set.Make (S.Var)
    module Thread_pool = Threadpool.Thread_pool

    open ParallelSolverStats (EqConstrSysFromCreatingEqConstrSys (S)) (HM)

    type state = {
      value: S.Dom.t;
      infl: VS.t;
      wpoint: bool;
      stable: bool;
      called: bool;
      root: bool;
    }
    let default () = {value = S.Dom.bot (); infl = VS.empty; called = false; stable = false; wpoint = false; root = false}

    let cas = CasStat.cas

    module DefaultState = struct
      type t = state
      let default = default
      let to_string s = S.Dom.show s.value
    end

    (* module CM = CreateOnlyConcurrentMap (S.Var) (DefaultState) (HM) *)
    module CM = Data.SafeHashmap (S.Var) (DefaultState) (HM)

    let create_empty_data () = CM.create () 

    let jobs_present_for = HM.create 1024

    let print_data data =
      Logs.debug "CAS success: %d" (Atomic.get CasStat.count_success);
      Logs.debug "CAS failure: %d" (Atomic.get CasStat.count_failure);
      Logs.debug "CAS success rate: %f" (float_of_int (Atomic.get CasStat.count_success) /. (float_of_int (Atomic.get CasStat.count_success + Atomic.get CasStat.count_failure)))
    (* Logs.debug "|rho|=%d" (LHM.length data) *)
    (*Logs.debug "|stable|=%d" (LHM.length data.stable);
      Logs.debug "|infl|=%d" (LHM.length data.infl);
      Logs.debug "|wpoint|=%d" (LHM.length data.wpoint)*)

    let print_data_verbose data str =
      if Logs.Level.should_log Debug then (
        Logs.debug "%s:" str;
        print_data data
      )

    let job_id_counter = (Atomic.make 1)

    let solve st vs =
      let nr_domains = GobConfig.get_int "solvers.td3.parallel_domains" in
      let nr_domains = if nr_domains = 0 then (Domain.recommended_domain_count ()) else nr_domains in

      (* domain with id 0 is always working. Threadpool initialized with n means domain 0 + n additional domains are working *)
      let pool = Thread_pool.create (nr_domains - 1) in

      let data = create_empty_data ()
      in

      (* let () = print_solver_stats := fun () -> *)
      (* print_data data *)
      (*Logs.info "|called|=%d" (LHM.length called);
        print_context_stats @@ LHM.to_hashtbl rho*)
      (* in *)

      let init x thread_id =
        let value, was_created = CM.find_create data x in
        (* TODO event: id is fixed to 0 *)
        if (was_created) then new_var_event thread_id x;
        value
      in

      let eq x get set create =
        if tracing then trace "eq" "eq %a" S.Var.pretty_trace x;
        match S.system x with
        | None -> S.Dom.bot ()
        | Some f -> f get set (Some create)
      in

      (** destabilizes vars from outer_w and their infl. Restarts from a root, if it was destabilized*)
      let rec destabilize prom outer_w =
        let rec destab_single y =
          let y_atom = CM.find data y in
          let s = Atomic.get y_atom in
          if not s.stable then (
            ()
          ) else if s.called then (
            let success = cas y_atom s {s with stable = false} in
            if success then (
              if tracing then trace "destab-v" "stable remove %a (root:%b, called:%b)" S.Var.pretty_trace y s.root s.called;
            ) else (
              destab_single y
            )
          ) else (
            (* destabilize infl *)
            let inner_w = s.infl in
            let success = cas y_atom s {s with infl = VS.empty; stable = false} in
            if success then (
              if tracing then trace "destab-v" "stable remove %a (root:%b, called:%b)" S.Var.pretty_trace y s.root s.called;
              if s.root then create_task prom y; (* This has to be y *)
              destabilize prom inner_w
            ) else (
              destab_single y
            )
          ) in
        VS.iter destab_single outer_w

      (** creates a task to solve for y *)
      and create_task outer_prom y =
        let work_fun () =
          let job_id = Atomic.fetch_and_add job_id_counter 1 in
          let y_atom = init y job_id in
          let s = Atomic.get y_atom in
          if s.called then (
            instant_return_event ()
          ) else (
            let success = Atomic.compare_and_set y_atom s {s with called = true; stable = true; root = true} in 
            if success then (
              if tracing then trace "thread_pool" "starting task %d to iterate %a" job_id S.Var.pretty_trace y;
              thread_starts_solve_event job_id;
              let inner_prom = ref [] in
              iterate None inner_prom y job_id y_atom;
              HM.remove jobs_present_for y;
              thread_ends_solve_event job_id;
              Thread_pool.await_all pool (!inner_prom);
              if tracing then trace "thread_pool" "finishing task %d" job_id;
            ) else (
              Logs.error "CAS FAIL HERE";
            )
          )
        in
        if (not (HM.mem jobs_present_for y)) then (
          if tracing then trace "create" "create_task %a" S.Var.pretty_trace y;
          create_task_event ();
          HM.replace jobs_present_for y ();
          outer_prom := Thread_pool.add_work pool work_fun :: (!outer_prom)
        )

      (** iterates to solve for x (invoked from query to orig if present) *)
      and iterate orig prom x job_id x_atom = (* ~(inner) solve in td3*)
        let rec query x y = (* ~eval in td3 *)
          (* Query with atomics: *)
          (* if anything is changed, query is repeated and the initial call *)
          (* has no side effects. *)
          (* Thus, imitating that the query just happend in a later point in time.  *)
          let y_atom = init y job_id in
          get_var_event y;
          let s = Atomic.get y_atom in
          if tracing then trace "sol_query" "%d entering query for %a; stable %b; called %b" job_id S.Var.pretty_trace y s.stable s.called;
          (* if tracing then trace "infl" "add_infl %a %a" S.Var.pretty_trace y S.Var.pretty_trace x; *)
          (* let s = {s with infl = (VS.add x s.infl)} in *)
          let s_with_infl = {s with infl = (VS.add x s.infl)} in

          if s.called then (
            if tracing then trace "infl" "add_infl %a %a" S.Var.pretty_trace y S.Var.pretty_trace x;
            let success = cas y_atom s {s_with_infl with wpoint=true} in
            if success then s.value else query x y
          ) 
          else if s.stable then (
            let success = cas y_atom s s_with_infl in 
            if success then s.value else query x y
          ) else (
            if S.system y = None then (
              let success = cas y_atom s {s_with_infl with stable = true} in
              if success then s.value else query x y
            ) else (
              let success = cas y_atom s {s_with_infl with stable = true; called=true} in
              if success then (
                iterate (Some x) prom y job_id y_atom;
                (Atomic.get y_atom).value
              )
              else query x y
            )
          )
          (* TODO maybe restore the tracing from below *)
          (* if tracing then trace "sol_query" " %d exiting query for %a" job_id S.Var.pretty_trace y; *)
          (* if tracing then trace "answer" "answer: %a" S.Dom.pretty s.value; *)
        in

        let rec side x y d = (* side from x to y; only to variables y w/o rhs; x only used for trace *)
          assert (S.system y = None);
          let y_atom = init y job_id in
          let s = Atomic.get y_atom in
          if tracing then trace "side" "%d side to %a from %a" job_id S.Var.pretty_trace y S.Var.pretty_trace x;
          if tracing then trace "side-v" "%d side to %a (wpx: %b) from %a ## value: %a" job_id S.Var.pretty_trace y s.wpoint S.Var.pretty_trace x S.Dom.pretty d;
          let old = s.value in
          if S.Dom.leq d old then (
            ()
          ) else (
            let widen a b =                 
              if M.tracing then M.trace "sidew" "%d side widen %a" job_id S.Var.pretty_trace y;
              S.Dom.widen a (S.Dom.join a b)    
            in 
            if tracing then trace "update" "%d side update %a with \n\t%a" job_id S.Var.pretty_trace x S.Dom.pretty (widen old d);
            let w = s.infl in
            let new_s = {s with value = (widen old d); stable = true; infl = VS.empty} in
            let success = cas y_atom s new_s in
            if success then (
              if tracing then trace "destab" "%d side destabilizing %a" job_id S.Var.pretty_trace y;
              update_var_event job_id y old (new_s.value);
              destabilize prom w
            ) else (
              side x y d
            )
          )
        in

        let create x y = (* create called from x on y *)
          if tracing then trace "create" "create from td_parallel_base was executed from %a on %a" S.Var.pretty_trace x S.Var.pretty_trace y;
          (* if Random.bool () then create_task prom y else ignore @@ query x y *)
          create_task prom y
        in

        (* begining of iterate*)
        assert (S.system x <> None);
        (* let x_atom = init x in *)
        let s = Atomic.get x_atom in
        if tracing then trace "iter" "%d iterate %a, stable: %b, wpoint: %b" job_id S.Var.pretty_trace x s.stable s.wpoint;
        let wp = s.wpoint in (* if x becomes a wpoint during eq, checking this will delay widening until next iterate *)
        let eqd = eq x (query x) (side x) (create x) in
        (* TODO event: id is fixed to 0 *)
        eval_rhs_event job_id x;
        let s = Atomic.get x_atom in
        let old = s.value in
        let wpd = (* d after box operator (if wp) *)
          if not wp then 
            eqd
          else (if tracing then trace "wpoint" "box widening %a" S.Var.pretty_trace x; box old eqd)
        in
        (* TODO: wrap S.Dom.equal in timing if a reasonable threadsafe timing becomes available *)
        if S.Dom.equal wpd old then (
          (* old = wpd*)
          if s.stable then (
            let infl = match orig with 
              | Some z -> (VS.add z s.infl)
              | None -> s.infl in
            let s_new = {s with infl = infl; called = false; wpoint = false} in
            let success = Atomic.compare_and_set x_atom s s_new in
            if not success then  (iterate[@tailcall]) orig prom x job_id x_atom
          ) else (
            let s_new = {s with stable = true} in
            let success = Atomic.compare_and_set x_atom s s_new in
            (* if not success, we retry the iteration to have intervention free execution, *)
            (* if success, we also reiterate, as unstable *)
            if tracing then trace "iter" "iterate still unstable %a" S.Var.pretty_trace x;
            (iterate[@tailcall]) orig prom x job_id x_atom
          )
        ) else (
          (* old != wpd*)
          let w = s.infl in
          if tracing then trace "update" "%d iterate update %a with \n\t%a" job_id S.Var.pretty_trace x S.Dom.pretty wpd;
          let new_s = {s with value = wpd; infl = VS.empty} in
          let success = cas x_atom s new_s in
          if success then (
            if tracing then trace "destab" "%d iterate destabilizing %a" job_id S.Var.pretty_trace x;
            update_var_event job_id x old wpd;
            destabilize prom w;

            let rec finalize () =
              let s = Atomic.get x_atom in

              if s.stable then (            
                let infl = match orig with 
                  | Some z -> (VS.add z s.infl)
                  | None -> s.infl in
                let new_s = {s with called = false; infl = infl} in
                let success = Atomic.compare_and_set x_atom s new_s in
                if not success then (finalize[@tailcall]) ()
              ) else (
                let new_s = {s with stable = true} in
                let success = Atomic.compare_and_set x_atom s new_s in 
                if success then (
                  if tracing then trace "iter" "iterate changed %a" S.Var.pretty_trace x;
                  (iterate[@tailcall]) orig prom x job_id x_atom
                ) else (finalize[@tailcall]) ()
              ) in
            finalize ()

          ) else (
            (iterate[@tailcall]) orig prom x job_id x_atom
          );
        )
      in

      let set_start (x,d) =
        let x_atom = init x 0 in
        let s = Atomic.get x_atom in
        Atomic.set x_atom {s with value = d; stable = true}
      in

      (* beginning of main solve *)
      start_event ();

      List.iter set_start st;

      List.iter (fun x -> ignore @@ init x 0) vs;
      (* If we have multiple start variables vs, we might solve v1, then while solving v2 we side some global which v1 depends on with a new value. Then v1 is no longer stable and we have to solve it again. *)
      let i = ref 0 in
      let rec solver () = (* as while loop in paper *)
        incr i;
        let unstable_vs = List.filter (fun v -> not (Atomic.get @@ CM.find data v).stable) vs in
        if unstable_vs <> [] then (
          if Logs.Level.should_log Debug then (
            if !i = 1 then Logs.newline ();
            Logs.debug "Unstable solver start vars in %d. phase:" !i;
            List.iter (fun v -> Logs.debug "\t%a" S.Var.pretty_trace v) unstable_vs;
            Logs.newline ();
            flush_all ();
          );
          List.iter (fun x -> 
              if tracing then trace "multivar" "solving for %a" S.Var.pretty_trace x;
              Domainslib.Task.run pool (fun () -> 
                  Logs.error "Running";
                  let promises = ref [] in
                  create_task promises x;
                  Thread_pool.await_all pool (!promises)
                )
            ) unstable_vs;
          solver ();
        )
      in
      solver ();
      Thread_pool.finished_with pool;


      Logs.info "CAS success: %d" (Atomic.get CasStat.count_success);
      Logs.info "CAS failure: %d" (Atomic.get CasStat.count_failure);
      Logs.info "CAS success rate: %f" (float_of_int (Atomic.get CasStat.count_success) /. (float_of_int (Atomic.get CasStat.count_success + Atomic.get CasStat.count_failure)));
      (* After termination, only those variables are stable which are
       * - reachable from any of the queried variables vs, or
       * - effected by side-effects and have no constraints on their own (this should be the case for all of our analyses). *)

      print_stats ();
      stop_event ();
      (* print_data_verbose data "Data after iterate completed"; *)

      let t = Unix.gettimeofday () in
      let data_ht = CM.to_hashtbl data in
      Logs.error "Conversion to hashtable took %f" (Unix.gettimeofday () -. t);
      let wpoint = HM.map (fun _ s -> s.wpoint) data_ht in

      if GobConfig.get_bool "dbg.print_wpoints" then (
        Logs.newline ();
        Logs.debug "Widening points:";
        HM.iter (fun k wp -> if wp then Logs.debug "%a" S.Var.pretty_trace k) wpoint;
        Logs.newline ();
      );
      (* TODO reenable *)
      (* if GobConfig.get_bool "dbg.timing.enabled" then LHM.print_stats data; *)

      (* TODO maybe we can save steps by doing map directly on CM *)
      HM.map (fun _ s -> s.value) data_ht
  end

let () =
  Selector.add_creating_eq_solver ("td_parallel_base_cmap",  (module PostSolver.CreatingEqIncrSolverFromCreatingEqSolver (Base)));
