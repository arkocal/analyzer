(** Terminating, parallelized top-down solver with side effects. ([td_parallel_base]). TODO: better Name*)

(** Top down solver that is parallelized. TODO: better description *)
(* Options:
 * - solvers.td3.parallel_domains (default: 0 - automatic selection): Maximal number of Domains that the solver can use in parallel.
 * TODO: support 'solvers.td3.remove-wpoint' option? currently it acts as if this option was always enabled *)

open Batteries
open ConstrSys
open Messages

open Parallel_util

module M = Messages

module Base : GenericCreatingEqSolver =
  functor (S:CreatingEqConstrSys) ->
  functor (HM:Hashtbl.S with type key = S.v) ->
  struct
    open SolverBox.Warrow (S.Dom)
    (* TODO: Maybe different solver stats according for CreatingEQsys is needed *)
    include Generic.SolverStats (EqConstrSysFromCreatingEqConstrSys (S)) (HM)
    module VS = Set.Make (S.Var)
    module LHM = LockableHashtbl (S.Var) (HM)

    type state = {
      value: S.Dom.t;
      infl: VS.t;
      wpoint: bool;
      stable: bool;
      called: bool;
      root: bool;
    }
    let default () = {value = S.Dom.bot (); infl = VS.empty; called = false; stable = false; wpoint = false; root = false}

    let create_empty_data () = 
      let map_size = GobConfig.get_int "solvers.td3.LHM_size" in
      LHM.create map_size

    let print_data data =
      Logs.debug "|rho|=%d" (LHM.length data)
    (*Logs.debug "|stable|=%d" (LHM.length data.stable);
      Logs.debug "|infl|=%d" (LHM.length data.infl);
      Logs.debug "|wpoint|=%d" (LHM.length data.wpoint)*)

    let print_data_verbose data str =
      if Logs.Level.should_log Debug then (
        Logs.debug "%s:" str;
        print_data data
      )

    let job_id_counter = (Atomic.make 10)

    let solve st vs =
      let nr_threads = GobConfig.get_int "solvers.td3.parallel_domains" in
      let nr_threads = if nr_threads = 0 then (Cpu.numcores ()) else nr_threads in

      let pool = Thread_pool.create nr_threads in

      let data = create_empty_data ()
      in

      let () = print_solver_stats := fun () ->
          print_data data
          (*Logs.info "|called|=%d" (LHM.length called);
            print_context_stats @@ LHM.to_hashtbl rho*)
      in

      let init x =
        if not (LHM.mem data x) then (
          if tracing then trace "init" "init %a" S.Var.pretty_trace x;
          new_var_event x;
          LHM.replace data x @@ default ()
        )
      in

      let eq x get set create =
        if tracing then trace "eq" "eq %a" S.Var.pretty_trace x;
        match S.system x with
        | None -> S.Dom.bot ()
        | Some f -> f get set (Some create)
      in

      (** destabilizes vars from outer_w and their infl. Restarts from a root, if it was destabilized*)
      let rec destabilize prom outer_w =
        VS.iter (fun y ->
            LHM.lock y data;
            let s = LHM.find data y in
            if not s.stable then (
              LHM.unlock y data
            ) else if s.called then (
              if tracing then trace "destab-v" "stable remove %a (root:%b, called:%b)" S.Var.pretty_trace y s.root s.called;
              let s = {s with stable = false} in
              LHM.replace data y s;
              LHM.unlock y data
            ) else (
              (* destabilize infl *)
              let inner_w = s.infl in
              if tracing then trace "destab-v" "stable remove %a (root:%b, called:%b)" S.Var.pretty_trace y s.root s.called;
              let s = {s with infl = VS.empty; stable = false} in
              LHM.replace data y s;
              LHM.unlock y data;
              if s.root then create_task prom y; (* This has to be y *)
              destabilize prom inner_w
            )
          ) outer_w

      (** creates a task to solve for y *)
      and create_task outer_prom y =
        let job_id = Atomic.fetch_and_add job_id_counter 1 in
        let work_fun () =
          LHM.lock y data;
          init y;
          let s = LHM.find data y in
          if s.called then (
            LHM.unlock y data;
          ) else (
            if tracing then trace "thread_pool" "starting task %d to iterate %a" job_id S.Var.pretty_trace y;
            let s = {s with called = true; stable = true; root = true} in
            LHM.replace data y s;
            LHM.unlock y data;
            let inner_prom = ref [] in
            iterate None inner_prom y job_id;
            Thread_pool.await_all pool (!inner_prom);
            if tracing then trace "thread_pool" "finishing task %d" job_id
          )
        in
        outer_prom := Thread_pool.add_work pool work_fun :: (!outer_prom)

      (** iterates to solve for x (invoked from query to orig if present) *)
      and iterate orig prom x job_id = (* ~(inner) solve in td3*)
        let query x y = (* ~eval in td3 *)
          LHM.lock y data;
          init y;
          get_var_event y;
          let s = LHM.find data y in
          if tracing then trace "sol_query" "%d entering query for %a; stable %b; called %b" job_id S.Var.pretty_trace y s.stable s.called;
          if tracing then trace "infl" "add_infl %a %a" S.Var.pretty_trace y S.Var.pretty_trace x;
          let s = {s with infl = (VS.add x s.infl)} in
          if s.called then (
            if tracing then trace "infl" "add_infl %a %a" S.Var.pretty_trace y S.Var.pretty_trace x;
            let s = {s with wpoint = true} in
            LHM.replace data y s
          ) else if s.stable then (
            LHM.replace data y s
          ) else (
            if S.system y = None then (
              let s = {s with stable = true} in
              LHM.replace data y s
            ) else (
              let s = {s with called = true; stable = true} in
              LHM.replace data y s;
              LHM.unlock y data;
              if tracing then trace "iter" "iterate called from query";
              iterate (Some x) prom y job_id;
              LHM.lock y data
            )
          );
          let s = LHM.find data y in
          LHM.unlock y data;
          if tracing then trace "sol_query" " %d exiting query for %a" job_id S.Var.pretty_trace y;
          if tracing then trace "answer" "answer: %a" S.Dom.pretty s.value;
          s.value
        in

        let side x y d = (* side from x to y; only to variables y w/o rhs; x only used for trace *)
          assert (S.system y = None);
          LHM.lock y data;
          init y;
          let s = LHM.find data y in
          if tracing then trace "side" "%d side to %a from %a" job_id S.Var.pretty_trace y S.Var.pretty_trace x;
          if tracing then trace "side-v" "%d side to %a (wpx: %b) from %a ## value: %a" job_id S.Var.pretty_trace y s.wpoint S.Var.pretty_trace x S.Dom.pretty d;
          let old = s.value in
          if S.Dom.leq d old then (
            LHM.unlock y data
          ) else (
            let widen a b =                 
              if M.tracing then M.trace "sidew" "%d side widen %a" job_id S.Var.pretty_trace y;
              S.Dom.widen a (S.Dom.join a b)    
            in 
            if tracing then trace "update" "%d side update %a with \n\t%a" job_id S.Var.pretty_trace x S.Dom.pretty (widen old d);
            let w = s.infl in
            let s = {s with value = (widen old d); stable = true; infl = VS.empty} in
            LHM.replace data y s;
            LHM.unlock y data;
            if tracing then trace "destab" "%d side destabilizing %a" job_id S.Var.pretty_trace y;
            destabilize prom w
          )
        in

        let create x y = (* create called from x on y *)
          if tracing then trace "create" "create from td_parallel_base was executed from %a on %a" S.Var.pretty_trace x S.Var.pretty_trace y;
          create_task prom y
        in

        (* begining of iterate*)
        assert (S.system x <> None);
        LHM.lock x data;
        init x;
        let s = LHM.find data x in
        if tracing then trace "iter" "%d iterate %a, stable: %b, wpoint: %b" job_id S.Var.pretty_trace x s.stable s.wpoint;
        let wp = s.wpoint in (* if x becomes a wpoint during eq, checking this will delay widening until next iterate *)
        LHM.unlock x data;
        if tracing then trace "eq" "eval eq for %a" S.Var.pretty_trace x;
        let eqd = eq x (query x) (side x) (create x) in
        LHM.lock x data;
        let s = LHM.find data x in
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
            let s = {s with infl = infl; called = false; wpoint = false} in
            LHM.replace data x s;
            LHM.unlock x data
          ) else (
            let s = {s with stable = true} in
            LHM.replace data x s;
            LHM.unlock x data;
            if tracing then trace "iter" "iterate still unstable %a" S.Var.pretty_trace x;
            (iterate[@tailcall]) orig prom x job_id
          )
        ) else (
          (* old != wpd*)
          let w = s.infl in
          if tracing then trace "update" "%d iterate update %a with \n\t%a" job_id S.Var.pretty_trace x S.Dom.pretty wpd;
          let s = {s with value = wpd; infl = VS.empty} in
          LHM.replace data x s;
          LHM.unlock x data;
          if tracing then trace "destab" "%d iterate destabilizing %a" job_id S.Var.pretty_trace x;
          destabilize prom w;
          LHM.lock x data;
          let s = LHM.find data x in
          if s.stable then (            
            let infl = match orig with 
              | Some z -> (VS.add z s.infl)
              | None -> s.infl in
            let s = {s with called = false; infl = infl} in
            LHM.replace data x s;
            LHM.unlock x data;
          ) else (
            let s = {s with stable = true} in
            LHM.replace data x s;
            LHM.unlock x data;
            if tracing then trace "iter" "iterate changed %a" S.Var.pretty_trace x;
            (iterate[@tailcall]) orig prom x job_id
          )
        )
      in

      let set_start (x,d) =
        init x;
        let s = LHM.find data x in
        LHM.replace data x {s with value = d; stable = true}
      in

      (* beginning of main solve *)
      start_event ();

      List.iter set_start st;

      List.iter init vs;
      (* If we have multiple start variables vs, we might solve v1, then while solving v2 we side some global which v1 depends on with a new value. Then v1 is no longer stable and we have to solve it again. *)
      let i = ref 0 in
      let rec solver () = (* as while loop in paper *)
        incr i;
        let unstable_vs = List.filter (fun v -> not (LHM.find data v).stable) vs in
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
      (* After termination, only those variables are stable which are
       * - reachable from any of the queried variables vs, or
       * - effected by side-effects and have no constraints on their own (this should be the case for all of our analyses). *)

      stop_event ();
      print_data_verbose data "Data after iterate completed";

      let data_ht = LHM.to_hashtbl data in
      let wpoint = HM.map (fun _ s -> s.wpoint) data_ht in

      if GobConfig.get_bool "dbg.print_wpoints" then (
        Logs.newline ();
        Logs.debug "Widening points:";
        HM.iter (fun k wp -> if wp then Logs.debug "%a" S.Var.pretty_trace k) wpoint;
        Logs.newline ();
      );
      if GobConfig.get_bool "dbg.timing.enabled" then LHM.print_stats data;

      HM.map (fun _ s -> s.value) data_ht
  end

let () =
  Selector.add_creating_eq_solver ("td_parallel_base",  (module PostSolver.CreatingEqIncrSolverFromCreatingEqSolver (Base)));
