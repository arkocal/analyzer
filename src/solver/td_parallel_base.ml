(** Terminating top-down solver with side effects. Baseline for comparisons with td_parallel solvers ([td_parallel_base]).

    @see <https://doi.org/10.1017/S0960129521000499> Seidl, H., Vogler, R. Three improvements to the top-down solver.
    @see <https://arxiv.org/abs/2209.10445> Interactive Abstract Interpretation: Reanalyzing Whole Programs for Cheap. *)

(** Terminating top down solver that is parallelized for some cases, where multiple unknowns have to be solved for a rhs. *)
(* TD3: see paper 'Three Improvements to the Top-Down Solver' https://dl.acm.org/doi/10.1145/3236950.3236967
 * Option solvers.td3.* (default) ? true : false (solver in paper):
 * - term (true) ? use phases for widen+narrow (TDside) : use box (TDwarrow)*)

open Batteries
open ConstrSys
open Messages

open Parallel_util

(* parameters - TODO: change to goblint options *)
let map_size = 1000

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

    type solver_data = {
      rho: S.Dom.t LHM.t;
      infl: VS.t LHM.t;
      wpoint: unit LHM.t;
      stable: unit LHM.t;
      called: unit LHM.t;
      root: unit LHM.t;
    }

    let create_empty_data () = {
      infl = LHM.create map_size;
      rho = LHM.create map_size;
      wpoint = LHM.create map_size;
      stable = LHM.create map_size;
      called = LHM.create map_size;
      root = LHM.create map_size;
    }

    let print_data data =
      Logs.debug "|rho|=%d" (LHM.length data.rho);
      Logs.debug "|stable|=%d" (LHM.length data.stable);
      Logs.debug "|infl|=%d" (LHM.length data.infl);
      Logs.debug "|wpoint|=%d" (LHM.length data.wpoint)

    let print_data_verbose data str =
      if Logs.Level.should_log Debug then (
        Logs.debug "%s:" str;
        print_data data
      )

    type phase = Widen | Narrow (*[@@deriving show] (* used in iterate *)*)

    let solve st vs =

      let nr_threads = GobConfig.get_int "solvers.td3.parallel_domains" in

      let data = create_empty_data ()
      in

      let infl = data.infl in
      let rho = data.rho in
      let wpoint = data.wpoint in
      let stable = data.stable in
      let called = data.called in
      let root = data.root in

      let term  = GobConfig.get_bool "solvers.td3.term" in
      let remove_wpoint = GobConfig.get_bool "solvers.td3.remove-wpoint" in

      let () = print_solver_stats := fun () ->
          print_data data;
          Logs.info "|called|=%d" (LHM.length called);
          print_context_stats @@ LHM.to_hashtbl rho
      in

      let add_infl y x =
        if tracing then trace "infl" "add_infl %a %a" S.Var.pretty_trace y S.Var.pretty_trace x;
        LHM.replace infl y (VS.add x (try LHM.find infl y with Not_found -> VS.empty));
      in

      let init x =
        if tracing then trace "sol2" "init %a" S.Var.pretty_trace x;
        if not (LHM.mem rho x) then (
          new_var_event x;
          LHM.replace rho x (S.Dom.bot ())
        )
      in

      let eq x get set create =
        if tracing then trace "sol2" "eq %a" S.Var.pretty_trace x;
        match S.system x with
        | None -> S.Dom.bot ()
        | Some f -> f get set (Some create)
      in

      let rec destabilize x =
        if tracing then trace "sol2" "destabilize %a" S.Var.pretty_trace x;
        let w = LHM.find_default infl x VS.empty in
        LHM.replace infl x VS.empty;
        VS.iter (fun y ->
            if tracing then trace "sol2" "stable remove %a" S.Var.pretty_trace y;
            if tracing then trace "destab" "destabilizing %a" S.Var.pretty_trace y;
            LHM.remove stable y;
            (* TODO: in td3 only deep destab when uncalled. Removed to be consistent with other solvers *)
            destabilize y
          ) w
      in

      let rec iterate ?reuse_eq x phase = (* ~(inner) solve in td3*)
        let query x y = (* ~eval in td3 *)
          let prio = if LHM.mem called y then 9 else 10 in
          if tracing then trace "called" "entering query with prio %d for %a" prio S.Var.pretty_trace y;
          let simple_solve y =
            if tracing then trace "sol2" "simple_solve %a (rhs: %b)" S.Var.pretty_trace y (S.system y <> None);
            if S.system y = None then (
              init y;
              LHM.replace stable y ()
            ) else (
              if tracing then trace "called" "query setting prio from 10 to 9 for %a" S.Var.pretty_trace y;
              LHM.replace called y ();
              if tracing then trace "iter" "iterate called from query";
              iterate y Widen;
              if tracing then trace "called" "query setting prio back from 9 to 10 for %a" S.Var.pretty_trace y;
              LHM.remove called y)
          in
          if tracing then trace "sol2" "query %a ## %a" S.Var.pretty_trace x S.Var.pretty_trace y;
          get_var_event y;
          if not (LHM.mem called y) then (
            simple_solve y
          ) else (
            if tracing then trace "sol2" "query adding wpoint %a from %a" S.Var.pretty_trace y S.Var.pretty_trace x;
            LHM.replace wpoint y ();
          );
          let tmp = LHM.find rho y in
          add_infl y x;
          if tracing then trace "sol2" "query %a ## %a -> %a" S.Var.pretty_trace x S.Var.pretty_trace y S.Dom.pretty tmp;
          if tracing then trace "called" "exiting query for %a" S.Var.pretty_trace y;
          tmp
        in

        let side x y d = (* side from x to y; only to variables y w/o rhs; x only used for trace *)
          if tracing then trace "sol2" "side to %a (wpx: %b) from %a ## value: %a" S.Var.pretty_trace y (LHM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty d;
          if S.system y <> None then (
            Logs.warn "side-effect to unknown w/ rhs: %a, contrib: %a" S.Var.pretty_trace y S.Dom.pretty d;
          );
          assert (S.system y = None);
          init y;
          let widen a b =
            if M.tracing then M.trace "sidew" "side widen %a" S.Var.pretty_trace y;
            if M.tracing then M.traceli "sol2" "side widen %a %a" S.Dom.pretty a S.Dom.pretty b;
            let r = S.Dom.widen a (S.Dom.join a b) in
            if M.tracing then M.traceu "sol2" "-> %a" S.Dom.pretty r;
            r
          in
          let op a b = if LHM.mem wpoint y then widen a b else S.Dom.join a b
          in
          let old = LHM.find rho y in
          let tmp = op old d in
          if tracing then trace "sol2" "stable add %a" S.Var.pretty_trace y;
          LHM.replace stable y ();
          if not (S.Dom.leq tmp old) then (
            if tracing && not (S.Dom.is_bot old) then trace "solside" "side to %a (wpx: %b) from %a: %a -> %a" S.Var.pretty_trace y (LHM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty old S.Dom.pretty tmp;
            if tracing && not (S.Dom.is_bot old) then trace "solchange" "side to %a (wpx: %b) from %a: %a" S.Var.pretty_trace y (LHM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty_diff (tmp, old);
            (* LHM.replace rho y ((if LHM.mem wpoint y then S.Dom.widen old else identity) (S.Dom.join old d)); *)
            LHM.replace rho y tmp;
            destabilize y;
            (* make y a widening point. This will only matter for the next side _ y.  *)
            if tracing then trace "sol2" "side adding wpoint %a from %a" S.Var.pretty_trace y S.Var.pretty_trace x;
            if tracing then trace "wpoint" "side adding wpoint %a (%b)" S.Var.pretty_trace y (LHM.mem wpoint y);
            LHM.replace wpoint y ()
          )
        in

        let create x y = (* create called from x on y *)
          if tracing then trace "create" "create from td_parallel_base was executed from %a on %a" S.Var.pretty_trace x S.Var.pretty_trace y;
          ignore (query x y)
        in

        (* begining of iterate*)
        if tracing then trace "iter" "iterate %a, called: %b, stable: %b, wpoint: %b" S.Var.pretty_trace x (LHM.mem called x) (LHM.mem stable x) (LHM.mem wpoint x);
        init x;
        assert (S.system x <> None);
        if not (LHM.mem stable x) then (
          if tracing then trace "sol2" "stable add %a" S.Var.pretty_trace x;
          LHM.replace stable x ();
          (* Here we cache LHM.mem wpoint x before eq. If during eq evaluation makes x wpoint, then be still don't apply widening the first time, but just overwrite.
             It means that the first iteration at wpoint is still precise.
             This doesn't matter during normal solving (?), because old would be bot.
             This matters during incremental loading, when wpoints have been removed (or not marshaled) and are redetected.
             Then the previous local wpoint value is discarded automagically and not joined/widened, providing limited restarting of local wpoints. (See query for more complete restarting.) *)
          let wp = LHM.mem wpoint x in (* if x becomes a wpoint during eq, checking this will delay widening until next iterate *)
          let eqd = (* d from equation/rhs *)
            match reuse_eq with
            | Some d ->
              (* Do not reset deps for reuse of eq *)
              if tracing then trace "sol2" "eq reused %a" S.Var.pretty_trace x;
              incr SolverStats.narrow_reuses;
              d
            | _ -> eq x (query x) (side x) (create x)
          in
          let old = LHM.find rho x in (* d from older iterate *) (* find old value after eq since wpoint restarting in eq/query might have changed it meanwhile *)
          let wpd = (* d after widen/narrow (if wp) *)
            if not wp then eqd
            else (
              if tracing then trace "wpoint" "widening %a" S.Var.pretty_trace x;
              if term then
                match phase with
                | Widen -> S.Dom.widen old (S.Dom.join old eqd)
                | Narrow when GobConfig.get_bool "exp.no-narrow" -> old (* no narrow *)
                | Narrow ->
                  (* assert S.Dom.(leq eqd old || not (leq old eqd)); (* https://github.com/goblint/analyzer/pull/490#discussion_r875554284 *) *)
                  S.Dom.narrow old eqd
              else
                box old eqd
            )
          in
          if tracing then trace "sol" "Var: %a (wp: %b)\nOld value: %a\nEqd: %a\nNew value: %a" S.Var.pretty_trace x wp S.Dom.pretty old S.Dom.pretty eqd S.Dom.pretty wpd;
          if not (Timing.wrap "S.Dom.equal" (fun () -> S.Dom.equal old wpd) ()) then ( 
            (* old != wpd *)
            if tracing then trace "sol" "Changed";
            (* if tracing && not (S.Dom.is_bot old) && LHM.mem wpoint x then trace "solchange" "%a (wpx: %b): %a -> %a" S.Var.pretty_trace x (LHM.mem wpoint x) S.Dom.pretty old S.Dom.pretty wpd; *)
            if tracing && not (S.Dom.is_bot old) && LHM.mem wpoint x then trace "solchange" "%a (wpx: %b): %a" S.Var.pretty_trace x (LHM.mem wpoint x) S.Dom.pretty_diff (wpd, old);
            update_var_event x old wpd;
            LHM.replace  rho x wpd;
            destabilize x;
            if tracing then trace "iter" "iterate changed";
            (iterate[@tailcall]) x phase
          ) else (
            (* old == wpd *)
            (* TODO: why non-equal and non-stable checks in switched order compared to TD3 paper? *)
            if not (LHM.mem stable x) then ( (* value unchanged, but not stable, i.e. destabilized itself during rhs *)
              if tracing then trace "sol2" "iterate still unstable %a" S.Var.pretty_trace x;
              if tracing then trace "iter" "iterate still unstable";
              (iterate[@tailcall]) x Widen
            ) else (
              if term && phase = Widen && LHM.mem wpoint x then ( (* TODO: or use wp? *)
                if tracing then trace "sol2" "iterate switching to narrow %a" S.Var.pretty_trace x;
                if tracing then trace "sol2" "stable remove %a" S.Var.pretty_trace x;
                LHM.remove stable x;
                if tracing then trace "iter" "iterate narrow";
                (iterate[@tailcall]) ~reuse_eq:eqd x Narrow
              ) else if remove_wpoint && (not term || phase = Narrow) then ( (* this makes e.g. nested loops precise, ex. tests/regression/34-localization/01-nested.c - if we do not remove wpoint, the inner loop head will stay a wpoint and widen the outer loop variable. *)
                if tracing then trace "sol2" "iterate removing wpoint %a (%b)" S.Var.pretty_trace x (LHM.mem wpoint x);
                LHM.remove wpoint x
              )
            )
          )
        )
      in

      let set_start (x,d) =
        if tracing then trace "sol2" "set_start %a ## %a" S.Var.pretty_trace x S.Dom.pretty d;
        init x;
        LHM.replace rho x d;
        LHM.replace stable x ();
        (* iterate x Widen *)
      in

      (* Imperative part starts here*)
      start_event ();

      List.iter set_start st;

      List.iter init vs;
      (* If we have multiple start variables vs, we might solve v1, then while solving v2 we side some global which v1 depends on with a new value. Then v1 is no longer stable and we have to solve it again. *)
      let i = ref 0 in
      let rec solver () = (* as while loop in paper *)
        incr i;
        let unstable_vs = List.filter (neg (LHM.mem stable)) vs in
        if unstable_vs <> [] then (
          if Logs.Level.should_log Debug then (
            if !i = 1 then Logs.newline ();
            Logs.debug "Unstable solver start vars in %d. phase:" !i;
            List.iter (fun v -> Logs.debug "\t%a" S.Var.pretty_trace v) unstable_vs;
            Logs.newline ();
            flush_all ();
          );
          List.iter (fun x -> LHM.replace called x ();
                      if tracing then trace "multivar" "solving for %a" S.Var.pretty_trace x;
                      iterate x Widen; LHM.remove called x) unstable_vs;
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

      if GobConfig.get_bool "dbg.print_wpoints" then (
        Logs.newline ();
        Logs.debug "Widening points:";
        LHM.iter (fun k () -> Logs.debug "%a" S.Var.pretty_trace k) wpoint;
        Logs.newline ();
      );


      print_data_verbose data "Data after postsolve";

      LHM.to_hashtbl rho
  end

let () =
  Selector.add_creating_eq_solver ("td_parallel_base",  (module PostSolver.CreatingEqIncrSolverFromCreatingEqSolver (Base)));
