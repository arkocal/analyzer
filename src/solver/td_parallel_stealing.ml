(** Terminating, parallelized top-down solver with side effects. ([td_parallel_stealing]).*)

(** Top down solver that is parallelized. TODO: better description *)
(* Options:
 * - solvers.td3.parallel_domains (default: 2): Maximal number of Domains that the solver can use in parallel.
 * TODO: support 'solvers.td3.remove-wpoint' option? currently it acts as if this option was always enabled *)

open Batteries
open ConstrSys
open Messages

open Parallel_util

(* parameter map_size for LHM - TODO: Fix a value or change to goblint option *)
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

    (* data of *)
    type solver_data = {
      rho: S.Dom.t LHM.t;
      called: int LHM.t;
      stable: int LHM.t;
    }

    type thread_data = {
      wpoint: unit HM.t;
      infl: VS.t HM.t;
    } 

    let create_empty_solver_data () = {
      rho = LHM.create map_size;
      called = LHM.create map_size;
      stable = LHM.create map_size;
    }

    let create_empty_thread_data () = {
      wpoint = HM.create 10;
      infl = HM.create 10;
    }

    let print_data data =
      Logs.debug "|rho|=%d" (LHM.length data.rho);
      Logs.debug "|called|=%d" (LHM.length data.called);
      Logs.debug "|stable|=%d" (LHM.length data.stable)

    let print_data_verbose data str =
      if Logs.Level.should_log Debug then (
        Logs.debug "%s:" str;
        print_data data
      )

    let solve st vs =

      let nr_threads = GobConfig.get_int "solvers.td3.parallel_domains" in
      let nr_threads = if nr_threads = 0 then (Cpu.numcores ()) else nr_threads in
      let highest_prio = 0 in 
      let lowest_prio = highest_prio + nr_threads + 1 in

      let data = create_empty_solver_data () in

      let rho = data.rho in
      let stable = data.stable in
      let called = data.called in

      let () = print_solver_stats := fun () ->
          print_data data;
          print_context_stats @@ LHM.to_hashtbl rho
      in

      let init x =
        if not (LHM.mem rho x) then (
          new_var_event x;
          if tracing then trace "init" "initializing %a" S.Var.pretty_trace x;
          LHM.replace rho x (S.Dom.bot ())
        )
      in

      let eq x get set =
        match S.system x with
        | None -> S.Dom.bot ()
        | Some f -> f get set
      in

      let solve_thread x prio =
        let t_data = create_empty_thread_data () in
        let wpoint = t_data.wpoint in
        let infl = t_data.infl in

        (* returns the current called/stable prio value of x. returns lowest_prio if not present *)
        let stable_prio x = LHM.find_default stable x lowest_prio in
        let called_prio x = LHM.find_default called x lowest_prio in

        let add_infl y x =
          if tracing then trace "infl" "%d add_infl %a %a" prio S.Var.pretty_trace y S.Var.pretty_trace x;
          HM.replace infl y (VS.add x (HM.find_default infl y VS.empty));
        in

        let rec destabilize ~all x =
          let w = HM.find_default infl x VS.empty in
          HM.replace infl x VS.empty;
          VS.iter (fun y ->
              LHM.lock y rho;
              if (all || (prio <= stable_prio y)) && (stable_prio y <> lowest_prio) then (
                if tracing then trace "destab" "%d destabilizing %a from %d" prio S.Var.pretty_trace y (stable_prio y);
                LHM.replace stable y lowest_prio
              );
              LHM.unlock y rho;
              destabilize ~all y
            ) w
        in

        let rec iterate x = (* ~(inner) solve in td3*)
          let query x y = (* ~eval in td3 *)
            if tracing then trace "sol_query" "%d entering query with prio %d for %a" prio (called_prio y) S.Var.pretty_trace y;
            LHM.lock y rho;
            get_var_event y;
            if not (called_prio y <= prio) then (
              (* If owning new, make sure it is not in point *)
              if tracing && (HM.mem wpoint y) then trace "wpoint" "%d query removing wpoint %a" prio S.Var.pretty_trace y;
              if HM.mem wpoint y then HM.remove wpoint y;
              init y;
              if S.system y = None then (
                if tracing then trace "stable" "%d query setting %a stable from %d" prio S.Var.pretty_trace y (stable_prio y);
                LHM.replace stable y prio
              ) else (
                if tracing then trace "called" "%d query setting prio from %d to %d for %a" prio (called_prio y) prio S.Var.pretty_trace y;
                if tracing then trace "own" "%d taking ownership of %a." prio S.Var.pretty_trace y;
                if tracing && ((called_prio y) != lowest_prio) then trace "steal" "%d stealing %a from %d" prio S.Var.pretty_trace y (called_prio y);
                LHM.replace called y prio;
                LHM.unlock y rho;
                (* call iterate unlocked *)
                if tracing then trace "iter" "%d iterate called from query" prio;
                iterate y;
                LHM.lock y rho;
                if (called_prio y >= prio) then (
                  if tracing then trace "own" "%d giving up ownership of %a." prio S.Var.pretty_trace y;
                  if tracing then trace "called" "%d query setting prio back from %d to %d for %a" prio (called_prio y) lowest_prio S.Var.pretty_trace y;
                  LHM.replace called y lowest_prio
                )
              )
            ) else (
              if tracing && not (HM.mem wpoint y) then trace "wpoint" "%d query adding wpoint %a from %a" prio S.Var.pretty_trace y S.Var.pretty_trace x;
              HM.replace wpoint y ()
            );
            let tmp = LHM.find rho y in
            LHM.unlock y rho;
            add_infl y x;
            if tracing then trace "sol_query" "%d exiting query for %a" prio S.Var.pretty_trace y;
            tmp
          in

          let side x y d = (* side from x to y; only to variables y w/o rhs; x only used for trace *)
            if tracing then trace "side" "%d side to %a (wpx: %b) from %a ## value: %a" prio S.Var.pretty_trace y (HM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty d;
            assert (S.system y = None);
            LHM.lock y rho;
            init y;

            (* begining of side *)
            if (called_prio y >= prio) then (
              if tracing then trace "called" "%d side setting prio from %d to %d for %a" prio (called_prio y) prio S.Var.pretty_trace y;
              if tracing then trace "ownSide" "%d side taking ownership of %a. Previously owned by %d" prio S.Var.pretty_trace y (called_prio y);
              LHM.replace called y prio;
              let old = LHM.find rho y in
              (* currently any side-effect after the first one will be widened *)
              let tmp = if HM.mem wpoint y then S.Dom.widen old (S.Dom.join old d) else S.Dom.join old d in
              if not (S.Dom.leq tmp old) then (
                if tracing then trace "updateSide" "%d side setting %a to %a" prio S.Var.pretty_trace y S.Dom.pretty tmp;
                LHM.replace rho y tmp;
                LHM.unlock y rho;
                if tracing then trace "destab" "%d destabilize called from side to %a" prio S.Var.pretty_trace y;
                destabilize ~all:true y;
                if tracing && not (HM.mem wpoint y) then trace "wpoint" "%d side adding wpoint %a from %a" prio S.Var.pretty_trace y S.Var.pretty_trace x;
                HM.replace wpoint y ()
              ) else (
                LHM.unlock y rho
              )
            ) else (
              LHM.unlock y rho
            )
          in

          (* begining of iterate *)
          LHM.lock x rho;
          init x;
          if tracing then trace "iter" "%d iterate %a, called: %b, stable: %b, wpoint: %b" prio S.Var.pretty_trace x (called_prio x <= prio) (stable_prio x <= prio) (HM.mem wpoint x);
          assert (S.system x <> None);
          if not (stable_prio x <= prio) then (
            if tracing then trace "stable" "%d iterate setting %a stable from %d" prio S.Var.pretty_trace x (stable_prio x);
            LHM.replace stable x prio;
            let wp = HM.mem wpoint x in (* if x becomes a wpoint during eq, checking this will delay widening until next iterate *)
            LHM.unlock x rho;
            let eqd = eq x (query x) (side x) in
            LHM.lock x rho;
            let old = LHM.find rho x in (* d from older iterate *) (* find old value after eq since wpoint restarting in eq/query might have changed it meanwhile *)
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
                LHM.replace rho x wpd;
                LHM.unlock x rho;
                if tracing then trace "destab" "%d destabilize called from iterate of %a" prio S.Var.pretty_trace x;
                destabilize ~all:false x;
                if tracing then trace "iter" "%d iterate changed" prio;
                (iterate[@tailcall]) x
              ) else (
                LHM.unlock x rho
              )
            ) else (
              (* old = wpd*)
              if not (stable_prio x <= prio) then (
                if tracing then trace "iter" "%d iterate still unstable" prio;
                LHM.unlock x rho;
                (iterate[@tailcall]) x
              ) else (
                if tracing && (HM.mem wpoint x) then trace "wpoint" "%d iterate removing wpoint %a" prio S.Var.pretty_trace x;
                HM.remove wpoint x;
                LHM.unlock x rho
              )
            )
          ) else (
            LHM.unlock x rho;
          )
        in
        iterate x;
      in

      let set_start (x,d) =
        init x;
        LHM.replace rho x d;
        LHM.replace stable x highest_prio;
      in

      let start_threads x =
        (* threads are created with a distinct prio, so that for all threads it holds: lowest_prio > prio > highest_prio  *)
        assert (nr_threads > 0);
        let threads = Array.init nr_threads (fun j ->
            Domain.spawn (fun () -> 
                if Logs.Level.should_log Debug then Printexc.record_backtrace true;
                Unix.sleepf (Float.mul (float j) 0.25);
                let prio = (lowest_prio - j - 1) in
                if tracing then trace "start" "thread %d with prio %d started" j prio;
                try
                  solve_thread x prio
                with 
                  e -> Printexc.print_backtrace stderr;
                  raise e
              )) in 
        Array.iter Domain.join threads
      in

      (* beginning of main solve *)
      start_event ();

      List.iter set_start st;

      List.iter init vs;
      (* If we have multiple start variables vs, we might solve v1, then while solving v2 we side some global which v1 depends on with a new value. Then v1 is no longer stable and we have to solve it again. *)
      let i = ref 0 in
      let rec solver () = (* as while loop in paper *)
        incr i;
        let unstable_vs = List.filter (neg (fun x -> LHM.find_default stable x lowest_prio < lowest_prio)) vs in
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
              start_threads x;
            ) unstable_vs;
          solver ();
        )
      in
      solver ();
      (* After termination, only those variables are stable which are
       * - reachable from any of the queried variables vs, or
       * - effected by side-effects and have no constraints on their own (this should be the case for all of our analyses). *)

      stop_event ();
      print_data_verbose data "Data after iterate completed";

      if GobConfig.get_bool "dbg.print_wpoints" then (
        Logs.newline ();
        Logs.debug "The threads of the td_parallel_stealing solver do not share widening points. They can currently not be printed.";
        Logs.newline ();
      );

      print_data_verbose data "Data after postsolve";

      LHM.to_hashtbl rho
  end

let () =
  Selector.add_solver ("td_parallel_stealing", (module PostSolver.EqIncrSolverFromEqSolver (Base)));
