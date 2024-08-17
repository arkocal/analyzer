(** Terminating, parallelized top-down solver with side effects ([td_parallel_dist]).

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

module M = Messages

module Base : GenericCreatingEqSolver =
  functor (S:CreatingEqConstrSys) ->
  functor (HM:Hashtbl.S with type key = S.v) ->
  struct
    open SolverBox.Warrow (S.Dom)
    (* TODO: Maybe different solver stats according for CreatingEQsys is needed *)
    include Generic.SolverStats (EqConstrSysFromCreatingEqConstrSys (S)) (HM)
    module VS = Set.Make (S.Var)

    module type Sides = sig
      type obs
      type res = NewSide | Fin

      val process_updates: obs -> ((S.Var.t * S.Dom.t) -> unit) -> obs
      val add_side: (unit -> unit) -> (S.Var.t * S.Dom.t) -> unit
      val no_observations: unit -> obs

      val updates_or_fin: (unit -> unit) -> obs -> res
    end

    module Sides: Sides = struct
      type obs = int
      type res = NewSide | Fin
      let next_obs = ref 1
      let sides = ref []
      let no_observations () = 0

      let mutex = Mutex.create ()
      let add_side revive (v,d) =
        Mutex.lock mutex;
        sides := (!next_obs,(v,d)) :: !sides;
        revive ();
        Mutex.unlock mutex

      let process_updates obs f =
        Mutex.lock mutex;
        let current_sides = !sides in
        (* Could also get this from the map, but we are too lazy for that *)
        let obs' = !next_obs -1 in
        Mutex.unlock mutex;
        let rec doit = function
          | [] -> ()
          | (o,_)::_ when o = obs -> ()
          | (_,(v,d))::tl -> f (v,d); doit tl
        in
        doit current_sides;
        obs'

      let updates_or_fin suspend obs =
        Mutex.lock mutex;
        if !next_obs-1 <> obs then
          begin
            Mutex.unlock mutex; NewSide
          end
        else
          begin
            suspend ();
            Mutex.unlock mutex;
            Fin
          end
    end

    type solver_data = {
      obs: Sides.obs ref;
      infl: VS.t HM.t;
      rho: S.Dom.t HM.t;
      wpoint: unit HM.t;
      stable: unit HM.t;
      called: unit HM.t
    }

    let create_empty_data () = {
      obs = ref (Sides.no_observations ());
      infl = HM.create 10;
      rho = HM.create 10;
      wpoint = HM.create 10;
      stable = HM.create 10;
      called = HM.create 10;
    }

    let print_data data =
      Logs.debug "|rho|=%d" (HM.length data.rho);
      Logs.debug "|stable|=%d" (HM.length data.stable);
      Logs.debug "|infl|=%d" (HM.length data.infl);
      Logs.debug "|wpoint|=%d" (HM.length data.wpoint)

    let print_data_verbose data str =
      if Logs.Level.should_log Debug then (
        Logs.debug "%s:" str;
        print_data data
      )

    type phase = Widen | Narrow [@@deriving show] (* used in iterate *)

    let solve st vs =

      let nr_threads = GobConfig.get_int "solvers.td3.parallel_domains" in

      let pool = Thread_pool.create nr_threads in

      let promises = ref [] in
      let prom_mutex = Mutex.create () in

      let created_vars = HM.create 10 in
      let suspended_vars = ref [] in
      let data = create_empty_data ()
      in

      let infl = data.infl in
      let rho = data.rho in
      let wpoint = data.wpoint in
      let stable = data.stable in
      let called = data.called in

      let term  = GobConfig.get_bool "solvers.td3.term" in
      let remove_wpoint = GobConfig.get_bool "solvers.td3.remove-wpoint" in

      let () = print_solver_stats := fun () ->
          print_data data;
          Logs.info "|called|=%d" (HM.length called);
          print_context_stats rho
      in

      let add_infl y x =
        if tracing then trace "sol2" "add_infl %a %a" S.Var.pretty_trace y S.Var.pretty_trace x;
        HM.replace infl y (VS.add x (try HM.find infl y with Not_found -> VS.empty));
      in

      let init x =
        if tracing then trace "sol2" "init %a" S.Var.pretty_trace x;
        if not (HM.mem rho x) then (
          new_var_event x;
          HM.replace rho x (S.Dom.bot ())
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
        let w = HM.find_default infl x VS.empty in
        HM.replace infl x VS.empty;
        VS.iter (fun y ->
            if tracing then trace "sol2" "stable remove %a" S.Var.pretty_trace y;
            HM.remove stable y;
            if not (HM.mem called y) then destabilize y
          ) w
      in

      let rec iterate ?reuse_eq x phase = (* ~(inner) solve in td3*)
        let query x y = (* ~eval in td3 *)
          let simple_solve y =
            if tracing then trace "sol2" "simple_solve %a (rhs: %b)" S.Var.pretty_trace y (S.system y <> None);
            if S.system y = None then (
              init y;
              HM.replace stable y ()
            ) else (
              HM.replace called y ();
              iterate y Widen;
              HM.remove called y)
          in
          if tracing then trace "sol2" "query %a ## %a" S.Var.pretty_trace x S.Var.pretty_trace y;
          get_var_event y;
          if not (HM.mem called y) then (
            simple_solve y
          ) else (
            if tracing then trace "sol2" "query adding wpoint %a from %a" S.Var.pretty_trace y S.Var.pretty_trace x;
            HM.replace wpoint y ();
          );
          let tmp = HM.find rho y in
          add_infl y x;
          if tracing then trace "sol2" "query %a ## %a -> %a" S.Var.pretty_trace x S.Var.pretty_trace y S.Dom.pretty tmp;
          tmp
        in

        let side x y d = (* side from x to y; only to variables y w/o rhs; x only used for trace *)
          if tracing then trace "sol2" "side to %a (wpx: %b) from %a ## value: %a" S.Var.pretty_trace y (HM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty d;
          if S.system y <> None then (
            Logs.warn "side-effect to unknown w/ rhs: %a, contrib: %a" S.Var.pretty_trace y S.Dom.pretty d;
          );
          assert (S.system y = None);
          init y;
          let widen a b =
            if M.tracing then M.traceli "sol2" "side widen %a %a" S.Dom.pretty a S.Dom.pretty b;
            let r = S.Dom.widen a (S.Dom.join a b) in
            if M.tracing then M.traceu "sol2" "-> %a" S.Dom.pretty r;
            r
          in
          let op a b = if HM.mem wpoint y then widen a b else S.Dom.join a b
          in
          let old = HM.find rho y in
          let tmp = op old d in
          if tracing then trace "sol2" "stable add %a" S.Var.pretty_trace y;
          HM.replace stable y ();
          if not (S.Dom.leq tmp old) then (
            if tracing && not (S.Dom.is_bot old) then trace "solside" "side to %a (wpx: %b) from %a: %a -> %a" S.Var.pretty_trace y (HM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty old S.Dom.pretty tmp;
            if tracing && not (S.Dom.is_bot old) then trace "solchange" "side to %a (wpx: %b) from %a: %a" S.Var.pretty_trace y (HM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty_diff (tmp, old);
            (* HM.replace rho y ((if HM.mem wpoint y then S.Dom.widen old else identity) (S.Dom.join old d)); *)
            HM.replace rho y tmp;
            destabilize y;
            (* make y a widening point. This will only matter for the next side _ y.  *)
            if tracing then trace "sol2" "side adding wpoint %a from %a" S.Var.pretty_trace y S.Var.pretty_trace x;
            HM.replace wpoint y ()
          )
        in  

        let create x y = (* create called from x on y *)
          if tracing then trace "create" "create from td_parallel_dist was executed from %a on %a" S.Var.pretty_trace x S.Var.pretty_trace y;
          ignore (query x y)
        in

        (* begining of iterate*)
        if tracing then trace "sol2" "iterate %a, phase: %s, called: %b, stable: %b, wpoint: %b" S.Var.pretty_trace x (show_phase phase) (HM.mem called x) (HM.mem stable x) (HM.mem wpoint x);
        init x;
        assert (S.system x <> None);
        if not (HM.mem stable x) then (
          if tracing then trace "sol2" "stable add %a" S.Var.pretty_trace x;
          HM.replace stable x ();
          (* Here we cache HM.mem wpoint x before eq. If during eq evaluation makes x wpoint, then be still don't apply widening the first time, but just overwrite.
             It means that the first iteration at wpoint is still precise.
             This doesn't matter during normal solving (?), because old would be bot.
             This matters during incremental loading, when wpoints have been removed (or not marshaled) and are redetected.
             Then the previous local wpoint value is discarded automagically and not joined/widened, providing limited restarting of local wpoints. (See query for more complete restarting.) *)
          let wp = HM.mem wpoint x in (* if x becomes a wpoint during eq, checking this will delay widening until next iterate *)
          let eqd = (* d from equation/rhs *)
            match reuse_eq with
            | Some d ->
              (* Do not reset deps for reuse of eq *)
              if tracing then trace "sol2" "eq reused %a" S.Var.pretty_trace x;
              incr SolverStats.narrow_reuses;
              d
            | _ -> eq x (query x) (side x) (create x)
          in
          let old = HM.find rho x in (* d from older iterate *) (* find old value after eq since wpoint restarting in eq/query might have changed it meanwhile *)
          let wpd = (* d after widen/narrow (if wp) *)
            if not wp then eqd
            else if term then
              match phase with
              | Widen -> S.Dom.widen old (S.Dom.join old eqd)
              | Narrow when GobConfig.get_bool "exp.no-narrow" -> old (* no narrow *)
              | Narrow ->
                (* assert S.Dom.(leq eqd old || not (leq old eqd)); (* https://github.com/goblint/analyzer/pull/490#discussion_r875554284 *) *)
                S.Dom.narrow old eqd
            else
              box old eqd
          in
          if tracing then trace "sol" "Var: %a (wp: %b)\nOld value: %a\nEqd: %a\nNew value: %a" S.Var.pretty_trace x wp S.Dom.pretty old S.Dom.pretty eqd S.Dom.pretty wpd;
          if not (Timing.wrap "S.Dom.equal" (fun () -> S.Dom.equal old wpd) ()) then ( (* value changed *)
            if tracing then trace "sol" "Changed";
            (* if tracing && not (S.Dom.is_bot old) && HM.mem wpoint x then trace "solchange" "%a (wpx: %b): %a -> %a" S.Var.pretty_trace x (HM.mem wpoint x) S.Dom.pretty old S.Dom.pretty wpd; *)
            if tracing && not (S.Dom.is_bot old) && HM.mem wpoint x then trace "solchange" "%a (wpx: %b): %a" S.Var.pretty_trace x (HM.mem wpoint x) S.Dom.pretty_diff (wpd, old);
            update_var_event x old wpd;
            HM.replace  rho x wpd;
            destabilize x;
            (iterate[@tailcall]) x phase
          ) else (
            (* TODO: why non-equal and non-stable checks in switched order compared to TD3 paper? *)
            if not (HM.mem stable x) then ( (* value unchanged, but not stable, i.e. destabilized itself during rhs *)
              if tracing then trace "sol2" "iterate still unstable %a" S.Var.pretty_trace x;
              (iterate[@tailcall]) x Widen
            ) else (
              if term && phase = Widen && HM.mem wpoint x then ( (* TODO: or use wp? *)
                if tracing then trace "sol2" "iterate switching to narrow %a" S.Var.pretty_trace x;
                if tracing then trace "sol2" "stable remove %a" S.Var.pretty_trace x;
                HM.remove stable x;
                (iterate[@tailcall]) ~reuse_eq:eqd x Narrow
              ) else if remove_wpoint && (not term || phase = Narrow) then ( (* this makes e.g. nested loops precise, ex. tests/regression/34-localization/01-nested.c - if we do not remove wpoint, the inner loop head will stay a wpoint and widen the outer loop variable. *)
                if tracing then trace "sol2" "iterate removing wpoint %a (%b)" S.Var.pretty_trace x (HM.mem wpoint x);
                HM.remove wpoint x
              )
            )
          )
        )
      in

      let set_start (x,d) =
        if tracing then trace "sol2" "set_start %a ## %a" S.Var.pretty_trace x S.Dom.pretty d;
        init x;
        HM.replace rho x d;
        HM.replace stable x ();
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
        let unstable_vs = List.filter (neg (HM.mem stable)) vs in
        if unstable_vs <> [] then (
          if Logs.Level.should_log Debug then (
            if !i = 1 then Logs.newline ();
            Logs.debug "Unstable solver start vars in %d. phase:" !i;
            List.iter (fun v -> Logs.debug "\t%a" S.Var.pretty_trace v) unstable_vs;
            Logs.newline ();
            flush_all ();
          );
          List.iter (fun x -> HM.replace called x (); iterate x Widen; HM.remove called x) unstable_vs;
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
        HM.iter (fun k () -> Logs.debug "%a" S.Var.pretty_trace k) wpoint;
        Logs.newline ();
      );


      print_data_verbose data "Data after postsolve";

      rho
  end

let () =
  Selector.add_creating_eq_solver ("td_parallel_dist", (module PostSolver.CreatingEqIncrSolverFromCreatingEqSolver (Base)));
