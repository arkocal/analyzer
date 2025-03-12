(** Terminating, parallelized top-down solver with side effects. ([td_parallel_stealing]).*)

(** Top down solver that is parallelized. TODO: better description *)
(* Options:
 * - solvers.td3.parallel_domains (default: 0 - automatic selection): Maximal number of Domains that the solver can use in parallel.
 * TODO: support 'solvers.td3.remove-wpoint' option? currently it acts as if this option was always enabled *)

open Batteries
open ConstrSys
open GobConfig
open ParallelStats
open Goblint_parallel
open Messages


module M = Messages


module Base : GenericCreatingEqSolver =
  functor (S:CreatingEqConstrSys) ->
  functor (HM:Hashtbl.S with type key = S.v) ->
  struct
    open SolverBox.Warrow (S.Dom)
    (* include Generic.SolverStats (EqConstrSysFromCreatingEqConstrSys (S)) (HM) *)
    include ParallelSolverStats (EqConstrSysFromCreatingEqConstrSys (S)) (HM)
    module VS = Set.Make (S.Var)

    type state = {
      value: S.Dom.t;
      called: int;
      stable: int;
    }

    module DefaultState = struct
      type t = state
      let default () = {value = S.Dom.bot (); called = Int.max_num; stable = Int.max_num} 
      let to_string s = Printf.sprintf "called: %d, stable: %d, value: %s" s.called s.stable (S.Dom.show s.value)
    end

    module CM = Data.SafeHashmap (S.Var) (DefaultState) (HM)

    type thread_data = {
      wpoint: unit HM.t;
      infl: VS.t HM.t;
    } 

    let create_empty_solver_data () = CM.create ()

    let create_empty_thread_data () = {
      wpoint = HM.create 10;
      infl = HM.create 10;
    }

    let print_data data = Logs.debug "Print data called"
    (*Logs.debug "|called|=%d" (LHM.length data.called);
        Logs.debug "|stable|=%d" (LHM.length data.stable)*)

    let print_data_verbose data str =
      if Logs.Level.should_log Debug then (
        Logs.debug "%s:" str;
        print_data data
      )

    let solve st vs =
      let stealing_revival = true in
      let nr_domains = GobConfig.get_int "solvers.td3.parallel_domains" in
      let nr_domains = if nr_domains = 0 then (Domain.recommended_domain_count ()) else nr_domains in
      let highest_prio = 0 in 
      let lowest_prio = Int.max_num in
      let main_finished = Atomic.make false in
      (* let default () = {value = S.Dom.bot (); called = lowest_prio; stable = lowest_prio} in *)

      let respawn_points = HM.create 10 in
      let data = create_empty_solver_data () in

      let () = print_solver_stats := fun () ->
          Logs.debug "New vars: %d" (Atomic.get vars);
      in

      (* Init with optional arg prio *)
      let init ?(prio=0) x =
        let value, was_created = CM.find_create data x in
        if (was_created) then new_var_event prio x;
        value
      in


      let eq x get set create =
        match S.system x with
        | None -> S.Dom.bot ()
        | Some f -> f get set (Some create)
      in

      let rec solve_thread x prio = 
        let init x = init ~prio:prio x in

        let rec find_work_from (worklist: S.v list) (seen: VS.t) = 
          if true || prio = 0 then (
            match worklist, prio with
            | [], _ -> None
            | x :: xs, 0 -> if tracing then trace "work" "%d (main) working on %a" prio S.Var.pretty_trace x; Some x
            | x :: xs, _ ->
              if tracing then trace "search" "%d searching for work %a" prio S.Var.pretty_trace x;
              let maybe_x_atom = CM.find_option data x in
              match maybe_x_atom with
              | None -> Some x
              | Some x_atom ->
                let xr = Atomic.get x_atom in
                let called_prio = xr.called in
                let stable_prio = xr.stable in
                let maybe_eq = S.system x in
                if (prio < called_prio && prio < stable_prio && Option.is_some maybe_eq) then (
                  if tracing then trace "work" "%d found work %a" prio S.Var.pretty_trace x;
                  Some x
                )
                else (
                  let new_work = ref [] in
                  let fw_query (y: S.v): S.d =
                    if (not (VS.mem y seen)) then new_work := y :: !new_work;
                    let y_atom = init y in
                    let s = Atomic.get y_atom in
                    s.value
                  in
                  let fw_side _ _ = () in
                  ignore @@ Option.map (fun eq -> eq fw_query fw_side) maybe_eq; 
                  (* eq x query side; *)
                  if Atomic.get main_finished then None else (
                    (* Introduce some randomness to prevent getting caught in unproductive sectors *)
                    let next_work = if (Random.bool ()) then (xs @ !new_work) else (!new_work @ xs) in
                    (* let next_work = xs @ !new_work in *)
                    (* let next_work = !new_work @ xs in *)
                    (* let next_work = if prio mod 2 = 0 then xs @ !new_work else !new_work @ xs in *)
                    find_work_from next_work (VS.add_seq (Seq.of_list !new_work) seen))
                )) else (
            while not @@ Atomic.get main_finished do () done;
            None
          )
        in 

        (* let rec find_work (worklist: S.v list) (seen: VS.t) =  *)
        (*   if (HM.is_empty respawn_points) then find_work_from worklist seen *)
        (*   else   *)
        (*     begin *)
        (*       let starting_point = fst @@ List.hd @@ HM.to_list respawn_points in *)
        (*       HM.remove respawn_points starting_point; *)
        (*       let found = find_work_from [starting_point] (VS.empty) in *)
        (*       match found with *)
        (*       | Some f -> Some f *)
        (*       | None -> find_work worklist seen *)
        (*       end *)
        (* in *)

        let find_work worklist seen =
          find_work_from ((HM.to_list respawn_points |> List.map fst) @ worklist) VS.empty
        in

        (* let find_work = find_work_from in *)

        let do_work x =
          (* Logs.info "Working on %a with prio %d" S.Var.pretty_trace x prio; *)
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
            let rec destab_single y =
              let y_atom = CM.find data y in
              let y_record = Atomic.get y_atom in
              if (all || (prio <= y_record.stable)) && (y_record.stable <> lowest_prio) then (
                if tracing then trace "destab" "%d destabilizing %a from %d" prio S.Var.pretty_trace y y_record.stable;
                let success = Atomic.compare_and_set y_atom y_record {y_record with stable = lowest_prio} in
                if not success then destab_single y
              ) in
            VS.iter (fun y -> destab_single y; destabilize ~all y) w
          in

          let rec iterate x = (* ~(inner) solve in td3*)
            let rec create x y = (* create called from x on y *)
              (* Logs.error "Create called!"; *)
              (* TODO this should also be thread safe *)
              if (prio == 0) then HM.replace respawn_points x ();
              (* Logs.error "Current number of respawn points: %d" (HM.length respawn_points); *)
              ignore @@ query x y;
              (* Logs.error "Create finished with prio %d" prio; *)
            and query x y = (* ~eval in td3 *)
              let y_atom = init y in
              get_var_event y;
              let s = Atomic.get y_atom in
              if tracing then trace "sol_query" "%d entering query with prio %d for %a" prio s.called S.Var.pretty_trace y;
              if not (s.called <= prio) then (
                (* If owning new, make sure it is not in point *)
                if tracing && (HM.mem wpoint y) then trace "wpoint" "%d query removing wpoint %a" prio S.Var.pretty_trace y;
                if HM.mem wpoint y then HM.remove wpoint y;
                if S.system y = None then ( (* Set globals to current priority *)
                  let success = Atomic.compare_and_set y_atom s {s with called = prio} in
                  if tracing && success then trace "stable" "%d query setting %a stable from %d" prio S.Var.pretty_trace y s.stable;
                ) else (
                  let success = Atomic.compare_and_set y_atom s {s with called = prio} in
                  if tracing && success then trace "own" "%d taking ownership of %a." prio S.Var.pretty_trace y;

                  if tracing && success then trace "iter" "%d iterate called from query" prio;
                  if success then iterate y;

                  let s = Atomic.get y_atom in
                  if (s.called >= prio) then (
                    if tracing then trace "own" "%d giving up ownership of %a." prio S.Var.pretty_trace y;
                    ignore @@ Atomic.compare_and_set y_atom s {s with called = lowest_prio};
                  )
                )
              ) else (
                if tracing && not (HM.mem wpoint y) then trace "wpoint" "%d query adding wpoint %a from %a" prio S.Var.pretty_trace y S.Var.pretty_trace x;
                HM.replace wpoint y ()
              );
              let s = Atomic.get y_atom in
              add_infl y x;
              if tracing then trace "answer" "%d query answer for %a: %a" prio S.Var.pretty_trace y S.Dom.pretty s.value;
              s.value
            in

            let side x y d = (* side from x to y; only to variables y w/o rhs; x only used for trace *)
              if tracing then trace "side" "%d side to %a (wpx: %b) from %a ## value: %a" prio S.Var.pretty_trace y (HM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty d;
              assert (S.system y = None);
              let y_atom = init y in
              let s = Atomic.get y_atom in
              if (s.called >= prio) then (
                let old = s.value in
                (* currently any side-effect after the first one will be widened *)
                let tmp = if HM.mem wpoint y then S.Dom.widen old (S.Dom.join old d) else S.Dom.join old d in
                if not (S.Dom.leq tmp old) then (
                  if tracing then trace "updateSide" "%d side setting %a to %a" prio S.Var.pretty_trace y S.Dom.pretty tmp;
                  if tracing then trace "ownSide" "%d side taking ownership of %a. Previously owned by %d" prio S.Var.pretty_trace y s.called;
                  let success = Atomic.compare_and_set y_atom s {s with value = tmp; called = prio} in
                  if (success) then (
                    update_var_event prio y old tmp;
                  );
                  (* TODO check if we are destabilizing too much *)
                  if tracing && success then trace "destab" "%d destabilize called from side to %a" prio S.Var.pretty_trace y;
                  destabilize ~all:true y;
                  if tracing && not (HM.mem wpoint y) then trace "wpoint" "%d side adding wpoint %a from %a" prio S.Var.pretty_trace y S.Var.pretty_trace x;
                  HM.replace wpoint y ()
                ) else (
                  let success = Atomic.compare_and_set y_atom s {s with called = prio} in
                  if tracing && success then trace "ownSide" "%d side taking ownership of %a. Previously owned by %d" prio S.Var.pretty_trace y s.called;
                )
              ) 
            in

            (* begining of iterate *)
            let x_atom = init x in
            let s = Atomic.get x_atom in
            if tracing then trace "iter" "%d iterate %a, called: %b, stable: %b, wpoint: %b" prio S.Var.pretty_trace x (s.called <= prio) (s.stable <= prio) (HM.mem wpoint x);
            assert (S.system x <> None);
            if not (s.stable <= prio) then (
              let set_stable_success = Atomic.compare_and_set x_atom s {s with stable = prio} in
              if tracing && set_stable_success then trace "stable" "%d iterate setting %a stable from %d" prio S.Var.pretty_trace x s.stable;
              let wp = HM.mem wpoint x in (* if x becomes a wpoint during eq, checking this will delay widening until next iterate *)
              let eqd = eq x (query x) (side x) (create x) in
              eval_rhs_event prio x;
              let s = Atomic.get x_atom in
              let old = s.value in (* d from older iterate *) (* find old value after eq since wpoint restarting in eq/query might have changed it meanwhile *)
              let wpd = (* d after box operator (if wp) *)
                if not wp then 
                  eqd
                else (if tracing then trace "wpoint" "%d box widening %a" prio S.Var.pretty_trace x; box old eqd)
              in
              (* TODO: wrap S.Dom.equal in timing if a reasonable threadsafe timing becomes available *)
              if set_stable_success && not (S.Dom.equal old wpd) then ( 
                (* old != wpd *)
                if (s.stable >= prio && s.called >= prio) then (
                  let value_success = Atomic.compare_and_set x_atom s {s with value = wpd} in 
                  if value_success then (
                    if tracing then trace "sol" "%d Changed" prio;
                    update_var_event prio x old wpd;
                    if tracing then trace "update" "%d setting %a to %a" prio S.Var.pretty_trace x S.Dom.pretty wpd;
                    if tracing then trace "destab" "%d destabilize called from iterate of %a" prio S.Var.pretty_trace x;
                    destabilize ~all:false x;
                    if tracing then trace "iter" "%d iterate changed" prio;
                    (iterate[@tailcall]) x
                  )
                )
              ) else (
                (* old = wpd*)
                if not (s.stable <= prio) then (
                  if tracing then trace "iter" "%d iterate still unstable" prio;
                  (iterate[@tailcall]) x
                ) else (
                  if tracing && (HM.mem wpoint x) then trace "wpoint" "%d iterate removing wpoint %a" prio S.Var.pretty_trace x;
                  HM.remove wpoint x;
                )
              )
            ) 
          in
          iterate x 
        in


        (* beginning of solve_thread *)
        if stealing_revival then (
          let to_iterate = find_work [ x ] VS.empty in
          begin match to_iterate with
            | Some x -> begin 
                (* TODO check if init is right here *)
                (* Logs.info "Found %a with prio %d" S.Var.pretty_trace x prio; *)
                let x_atom = init x in
                let s = Atomic.get x_atom in
                if prio < s.called then (
                  let success = Atomic.compare_and_set x_atom s {s with called = prio} in
                  if success then do_work x 
                )
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
        ) else (
          do_work x
        )
      in

      let set_start (x,d) =
        let x_atom = init x in
        let s = Atomic.get x_atom in
        ignore @@ Atomic.compare_and_set x_atom s {s with value = d; stable = highest_prio};
      in

      let start_threads x =
        (* threads are created with a distinct prio, so that for all threads it holds: lowest_prio > prio >= highest_prio. highest_prio is the main thread. *)
        Logs.info "Starting %d domains" nr_domains;
        let threads = Array.init (nr_domains-1) (fun j ->
            Domain.spawn (fun () -> 
                if Logs.Level.should_log Debug then Printexc.record_backtrace true;
                (* Unix.sleepf (Float.mul (float j) 0.25); *)
                (* let prio = (nr_domains - j - 1) in *)
                let prio = j+1 in
                if tracing then trace "start" "thread %d with prio %d started" j prio;
                try
                  solve_thread x prio
                with 
                  e -> Printexc.print_backtrace stderr;
                  raise e
              )) in 
        solve_thread x highest_prio;
        Array.iter Domain.join threads
      in

      (* beginning of main solve *)
      start_event ();

      List.iter set_start st;

      List.iter (fun x -> ignore @@ init x) vs;
      (* If we have multiple start variables vs, we might solve v1, then while solving v2 we side some global which v1 depends on with a new value. Then v1 is no longer stable and we have to solve it again. *)
      let i = ref 0 in
      let rec solver () = (* as while loop in paper *)
        incr i;
        let unstable_vs = List.filter (neg (fun x -> (Atomic.get @@ CM.find data x).stable < lowest_prio)) vs in
        if unstable_vs <> [] then (
          if Logs.Level.should_log Debug then (
            if !i = 1 then Logs.newline ();
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
      print_stats ();
      print_data_verbose data "Data after iterate completed";

      if GobConfig.get_bool "dbg.print_wpoints" then (
        Logs.newline ();
        Logs.debug "The threads of the td_parallel_stealing solver do not share widening points. They can currently not be printed.";
        Logs.newline ();
      );

      (* if GobConfig.get_bool "dbg.timing.enabled" then LHM.print_stats data; *)

      let data_ht = CM.to_hashtbl data in
      HM.map (fun _ s -> s.value) data_ht
  end

let () =
  Selector.add_creating_eq_solver ("td_parallel_stealing_cmap", (module PostSolver.CreatingEqIncrSolverFromCreatingEqSolver (Base)));
