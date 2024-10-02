(** Terminating, parallelized top-down solver with side effects. ([td_parallel_stealing]).*)

(** Top down solver that is parallelized. TODO: better description *)
(* Options:
 * - solvers.td3.parallel_domains (default: 0 - automatic selection): Maximal number of Domains that the solver can use in parallel.
 * TODO: support 'solvers.td3.remove-wpoint' option? currently it acts as if this option was always enabled *)

open Batteries
open ConstrSys
open Messages

open Parallel_util

module M = Messages

module Base : GenericEqSolver =
  functor (S:EqConstrSys) ->
  functor (HM:Hashtbl.S with type key = S.v) ->
  struct
    open SolverBox.Warrow (S.Dom)
    include Generic.SolverStats (S) (HM)
    module VS = Set.Make (S.Var)
    module LHM = LockableHashtbl (S.Var) (HM)

    type state = {
      value: S.Dom.t;
      called: int;
      stable: int;
    }

    type thread_data = {
      wpoint: unit HM.t;
      infl: VS.t HM.t;
    } 

    let create_empty_solver_data () =
      let map_size = GobConfig.get_int "solvers.td3.LHM_size" in
      LHM.create map_size

    let create_empty_thread_data () = {
      wpoint = HM.create 10;
      infl = HM.create 10;
    }

    let print_data data =
      Logs.debug "|rho|=%d" (LHM.length data)
    (*Logs.debug "|called|=%d" (LHM.length data.called);
      Logs.debug "|stable|=%d" (LHM.length data.stable)*)

    let print_data_verbose data str =
      if Logs.Level.should_log Debug then (
        Logs.debug "%s:" str;
        print_data data
      )

    let solve st vs =
      let nr_threads = GobConfig.get_int "solvers.td3.parallel_domains" in
      let nr_threads = if nr_threads = 0 then (Cpu.numcores ()) else nr_threads in
      let highest_prio = 0 in 
      let lowest_prio = nr_threads in
      let main_finished = Atomic.make false in
      let default () = {value = S.Dom.bot (); called = lowest_prio; stable = lowest_prio} in

      let data = create_empty_solver_data () in

      (*let () = print_solver_stats := fun () ->
          print_data data;
          print_context_stats @@ LHM.to_hashtbl rho
        in*)

      let init x =
        if not (LHM.mem data x) then (
          if tracing then trace "init" "init %a" S.Var.pretty_trace x;
          new_var_event x;
          LHM.replace data x @@ default ()
        )
      in

      let eq x get set =
        match S.system x with
        | None -> S.Dom.bot ()
        | Some f -> f get set
      in

      let rec solve_thread x prio = 

      let rec find_work (worklist: S.v list) (seen: VS.t) = 
        match worklist, prio with
        | [], _ -> None
        | x :: xs, 0 -> Some x
        | x :: xs, _ ->
          if tracing then trace "search" "%d searching for work %a" prio S.Var.pretty_trace x;
          LHM.lock x data;
          let xr = LHM.find_default data x {
            called = lowest_prio;
            value = S.Dom.bot ();
            stable = lowest_prio;
          } in
          let called_prio = xr.called in
          let stable_prio = xr.stable in
          LHM.unlock x data;
          let maybe_eq = S.system x in
          if (prio < called_prio && prio < stable_prio && Option.is_some maybe_eq) then (
            if tracing then trace "work" "%d found work %a" prio S.Var.pretty_trace x;
            Some x
          )
          else (
            let new_work = ref [] in
            let query (y: S.v): S.d =
              if (not (VS.mem y seen)) then new_work := y :: !new_work;
              LHM.lock y data;
              let s = LHM.find_default data y {
                called = lowest_prio;
                value = S.Dom.bot ();
                stable = lowest_prio;
              } in
              LHM.unlock y data;
              s.value
            in
            let side _ _ = () in
            ignore @@ Option.map (fun eq -> eq query side) maybe_eq; 
              (* eq x query side; *)
            if Atomic.get main_finished then None else (
              (* Introduce some randomness to prevent getting caught in unproductive sectors *)
              let next_work = if (Random.bool ()) then (xs @ !new_work) else (!new_work @ xs) in
              find_work next_work (VS.add_seq (Seq.of_list !new_work) seen))
          )
      in 

      let do_work x =
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
              LHM.lock y data;
              let s = LHM.find data y in
              if (all || (prio <= s.stable)) && (s.stable <> lowest_prio) then (
                if tracing then trace "destab" "%d destabilizing %a from %d" prio S.Var.pretty_trace y s.stable;
                LHM.replace data y {s with stable = lowest_prio}
              );
              LHM.unlock y data;
              destabilize ~all y
            ) w
        in

        let rec iterate x = (* ~(inner) solve in td3*)
          let query x y = (* ~eval in td3 *)
            LHM.lock y data;
            init y;
            get_var_event y;
            let s = LHM.find data y in
            if tracing then trace "sol_query" "%d entering query with prio %d for %a" prio s.called S.Var.pretty_trace y;
            if not (s.called <= prio) then (
              (* If owning new, make sure it is not in point *)
              if tracing && (HM.mem wpoint y) then trace "wpoint" "%d query removing wpoint %a" prio S.Var.pretty_trace y;
              if HM.mem wpoint y then HM.remove wpoint y;
              if S.system y = None then (
                if tracing then trace "stable" "%d query setting %a stable from %d" prio S.Var.pretty_trace y s.stable;
                LHM.replace data y {s with stable = prio}
              ) else (
                if tracing then trace "own" "%d taking ownership of %a." prio S.Var.pretty_trace y;
                if tracing && (s.called != lowest_prio) then trace "steal" "%d stealing %a from %d" prio S.Var.pretty_trace y s.called;
                LHM.replace data y {s with called = prio};
                LHM.unlock y data;
                (* call iterate unlocked *)
                if tracing then trace "iter" "%d iterate called from query" prio;
                iterate y;
                LHM.lock y data;
                let s = LHM.find data y in
                if (s.called >= prio) then (
                  if tracing then trace "own" "%d giving up ownership of %a." prio S.Var.pretty_trace y;
                  LHM.replace data y {s with called = lowest_prio}
                )
              )
            ) else (
              if tracing && not (HM.mem wpoint y) then trace "wpoint" "%d query adding wpoint %a from %a" prio S.Var.pretty_trace y S.Var.pretty_trace x;
              HM.replace wpoint y ()
            );
            let s = LHM.find data y in
            LHM.unlock y data;
            add_infl y x;
            if tracing then trace "answer" "%d query answer for %a: %a" prio S.Var.pretty_trace y S.Dom.pretty s.value;
            s.value
          in

          let side x y d = (* side from x to y; only to variables y w/o rhs; x only used for trace *)
            if tracing then trace "side" "%d side to %a (wpx: %b) from %a ## value: %a" prio S.Var.pretty_trace y (HM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty d;
            assert (S.system y = None);
            LHM.lock y data;
            init y;
            let s = LHM.find data y in
            if (s.called >= prio) then (
              let old = s.value in
              (* currently any side-effect after the first one will be widened *)
              let tmp = if HM.mem wpoint y then S.Dom.widen old (S.Dom.join old d) else S.Dom.join old d in
              if not (S.Dom.leq tmp old) then (
                if tracing then trace "updateSide" "%d side setting %a to %a" prio S.Var.pretty_trace y S.Dom.pretty tmp;
                if tracing then trace "ownSide" "%d side taking ownership of %a. Previously owned by %d" prio S.Var.pretty_trace y s.called;
                LHM.replace data y {s with value = tmp; called = prio};
                LHM.unlock y data;
                if tracing then trace "destab" "%d destabilize called from side to %a" prio S.Var.pretty_trace y;
                destabilize ~all:true y;
                if tracing && not (HM.mem wpoint y) then trace "wpoint" "%d side adding wpoint %a from %a" prio S.Var.pretty_trace y S.Var.pretty_trace x;
                HM.replace wpoint y ()
              ) else (
                if tracing then trace "ownSide" "%d side taking ownership of %a. Previously owned by %d" prio S.Var.pretty_trace y s.called;
                LHM.replace data y {s with called = prio};
                LHM.unlock y data
              )
            ) else (
              LHM.unlock y data
            )
          in

          (* begining of iterate *)
          LHM.lock x data;
          init x;
          let s = LHM.find data x in
          if tracing then trace "iter" "%d iterate %a, called: %b, stable: %b, wpoint: %b" prio S.Var.pretty_trace x (s.called <= prio) (s.stable <= prio) (HM.mem wpoint x);
          assert (S.system x <> None);
          if not (s.stable <= prio) then (
            if tracing then trace "stable" "%d iterate setting %a stable from %d" prio S.Var.pretty_trace x s.stable;
            LHM.replace data x {s with stable = prio};
            let wp = HM.mem wpoint x in (* if x becomes a wpoint during eq, checking this will delay widening until next iterate *)
            LHM.unlock x data;
            let eqd = eq x (query x) (side x) in
            LHM.lock x data;
            let s = LHM.find data x in
            let old = s.value in (* d from older iterate *) (* find old value after eq since wpoint restarting in eq/query might have changed it meanwhile *)
            let wpd = (* d after box operator (if wp) *)
              if not wp then 
                eqd
              else (if tracing then trace "wpoint" "%d box widening %a" prio S.Var.pretty_trace x; box old eqd)
            in
            (* TODO: wrap S.Dom.equal in timing if a reasonable threadsafe timing becomes available *)
            if not (S.Dom.equal old wpd) then ( 
              (* old != wpd *)
              if (s.stable >= prio && s.called >= prio) then (
                if tracing then trace "sol" "%d Changed" prio;
                update_var_event x old wpd;
                if tracing then trace "update" "%d setting %a to %a" prio S.Var.pretty_trace x S.Dom.pretty wpd;
                LHM.replace data x {s with value = wpd};
                LHM.unlock x data;
                if tracing then trace "destab" "%d destabilize called from iterate of %a" prio S.Var.pretty_trace x;
                destabilize ~all:false x;
                if tracing then trace "iter" "%d iterate changed" prio;
                (iterate[@tailcall]) x
              ) else (
                LHM.unlock x data
              )
            ) else (
              (* old = wpd*)
              if not (s.stable <= prio) then (
                if tracing then trace "iter" "%d iterate still unstable" prio;
                LHM.unlock x data;
                (iterate[@tailcall]) x
              ) else (
                if tracing && (HM.mem wpoint x) then trace "wpoint" "%d iterate removing wpoint %a" prio S.Var.pretty_trace x;
                HM.remove wpoint x;
                LHM.unlock x data
              )
            )
          ) else (
            LHM.unlock x data;
          )
        in
      iterate x 
      in

      let to_iterate = find_work [ x ] VS.empty in
      begin match to_iterate with
        | Some x -> begin 
          LHM.lock x data;
          let s = LHM.find_default data x {
            called = lowest_prio;
            value = S.Dom.bot ();
            stable = lowest_prio;
          } in
          if prio < s.called then LHM.replace data x {s with called = prio};
          LHM.unlock x data;
          do_work x
          end
        | None -> () end;
      if (prio > highest_prio) then ( 
        if (not @@ Atomic.get main_finished) then (
          (solve_thread[@tailcall]) x prio
        )
      )
      else (
        Atomic.set main_finished true;
      )
      in

      let set_start (x,d) =
        init x;
        let s = LHM.find data x in
        LHM.replace data x {s with value = d; stable = highest_prio}
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
        let unstable_vs = List.filter (neg (fun x -> (LHM.find data x).stable < lowest_prio)) vs in
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

      if GobConfig.get_bool "dbg.timing.enabled" then LHM.print_stats data;

      let data_ht = LHM.to_hashtbl data in
      HM.map (fun _ s -> s.value) data_ht
  end

let () =
  Selector.add_solver ("td_parallel_stealing", (module PostSolver.EqIncrSolverFromEqSolver (Base)));
