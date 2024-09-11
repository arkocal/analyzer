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

      let mutex = GobMutex.create ()

      let add_side revive (v,d) =
        GobMutex.lock mutex;
        sides := (!next_obs,(v,d)) :: !sides;
        revive ();
        GobMutex.unlock mutex

      let process_updates obs f =
        GobMutex.lock mutex;
        let current_sides = !sides in
        (* Could also get this from the map, but we are too lazy for that *)
        let obs' = !next_obs-1 in
        GobMutex.unlock mutex;
        let rec doit = function
          | [] -> ()
          | (o,_)::_ when o = obs -> ()
          | (_,(v,d))::tl -> 
            f (v,d); 
            doit tl
        in
        doit current_sides;
        obs'

      let updates_or_fin suspend obs =
        GobMutex.lock mutex;
        if tracing then trace "suspend" "next_obs = %d; obs = %d" !next_obs obs;
        if !next_obs-1 <> obs then
          begin
            GobMutex.unlock mutex; 
            NewSide
          end
        else
          begin
            suspend ();
            GobMutex.unlock mutex;
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
      Logs.info "|called|=%d" (HM.length data.called);
      Logs.debug "|rho|=%d" (HM.length data.rho);
      Logs.debug "|stable|=%d" (HM.length data.stable);
      Logs.debug "|infl|=%d" (HM.length data.infl);
      Logs.debug "|wpoint|=%d" (HM.length data.wpoint)

    let print_data_verbose data str =
      if Logs.Level.should_log Debug then (
        Logs.debug "%s:" str;
        print_data data
      )

    let solve st vs =
      let nr_threads = GobConfig.get_int "solvers.td3.parallel_domains" in

      let pool = Thread_pool.create nr_threads in

      let promises = ref [] in
      let prom_mutex = GobMutex.create () in

      let created_vars = HM.create 10 in
      let suspended_vars = ref [] in (* TODO other Data structure than list? Set?*)

      let job_id_counter = (Atomic.make 10) in

      (* TODO: make something reasonable out of this
         let () = print_solver_stats := fun () ->
          print_data data;
          Logs.info "|called|=%d" (HM.length called);
          print_context_stats rho
         in *)

      let init rho x =
        if not (HM.mem rho x) then (
          if tracing then trace "init" "init %a" S.Var.pretty_trace x;
          new_var_event x;
          HM.replace rho x (S.Dom.bot ())
        )
      in

      (** solves for a single point-of-interest variable (x_poi) *)
      let rec solve_single is_primary x_poi sd =
        let job_id = Atomic.fetch_and_add job_id_counter 1 in
        if tracing then trace "thread_pool" "starting task (primary:%b) %d to solve for %a" is_primary job_id S.Var.pretty_trace x_poi;

        let obs = sd.obs in
        let stable = sd.stable in
        let called = sd.called in
        let infl = sd.infl in
        let rho = sd.rho in
        let wpoint = sd.wpoint in

        let add_infl y x =
          if tracing then trace "infl" "add_infl %a %a" S.Var.pretty_trace y S.Var.pretty_trace x;
          HM.replace infl y (VS.add x (HM.find_default infl y VS.empty));
        in

        let eq x get set create =
          if tracing then trace "eq" "eq %a" S.Var.pretty_trace x;
          match S.system x with
          | None -> S.Dom.bot ()
          | Some f -> f get set (Some create)
        in

        let rec destabilize outer_w =
          VS.iter (fun y ->
              if not (HM.mem stable y) then
                ()
              else if HM.mem called y then
                HM.remove stable y
              else (
                let inner_w = HM.find_default infl y VS.empty in
                HM.replace infl y VS.empty;
                HM.remove stable y;
                destabilize inner_w
              )
            ) outer_w
        in

        (** iterates to solve for x *)
        let rec iterate orig x = (* ~(inner) solve in td3*)
          let query x y = (* ~eval in td3 *)
            if tracing then trace "sol_query" "%d entering query for %a; stable %b; called %b" job_id S.Var.pretty_trace y (HM.mem stable y) (HM.mem called y);
            if HM.mem stable y || HM.mem called y then (
              if HM.mem called y then HM.replace wpoint y ();
              add_infl y x
            ) else (
              if S.system y = None then (
                init rho y;
                HM.replace stable y ();
                obs := Sides.process_updates !obs handle_side; (* For vars without constraints, we need to handle sides here *)
              ) else (
                HM.replace stable y ();
                HM.replace called y ();
                iterate (Some x) y
              )
            );
            let tmp = HM.find rho y in
            if tracing then trace "answer" "exiting query for %a\nanswer: %a" S.Var.pretty_trace y S.Dom.pretty tmp;
            tmp
          in

          let side x y d = (* side from x to y; only to variables y w/o rhs; x only used for trace *)
            if tracing then trace "side" "side to %a (wpx: %b) from %a ## value: %a" S.Var.pretty_trace y (HM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty d;
            do_side y d
          in

          let create x y = (* create called from x on y *)
            if tracing then trace "create" "create from td_parallel_dist was executed from %a on %a" S.Var.pretty_trace x S.Var.pretty_trace y;
            GobMutex.lock prom_mutex;
            if HM.mem created_vars y then
              ()
            else (
              HM.replace created_vars y ();
              let sd = create_empty_data () in
              promises := (Thread_pool.add_work pool (fun () -> solve_single false y sd))::!promises
            );
            GobMutex.unlock prom_mutex
          in

          (* begining of iterate*)
          if tracing then trace "sol2" "iterate %a, called: %b, stable: %b, wpoint: %b" S.Var.pretty_trace x (HM.mem called x) (HM.mem stable x) (HM.mem wpoint x);
          init rho x;
          assert (S.system x <> None);
          let wp = HM.mem wpoint x in (* if x becomes a wpoint during eq, checking this will delay widening until next iterate *)
          let eqd = eq x (query x) (side x) (create x) in
          obs := Sides.process_updates !obs handle_side;
          let old = HM.find rho x in
          let wpd = (* d after widen/narrow (if wp) *)
            if not wp then eqd
            else box old eqd
          in
          if (Timing.wrap "S.Dom.equal" (fun () -> S.Dom.equal old wpd) ()) then (
            (* old = wpd*)
            if HM.mem stable x then (
              Option.may (add_infl x) orig;
              HM.remove called x;
              HM.remove wpoint x
            ) else (
              HM.replace stable x ();
              (iterate[@tailcall]) orig x
            )
          ) else (
            (* old != wpd*)
            let w = HM.find_default infl x VS.empty in
            HM.replace infl x VS.empty;
            HM.replace rho x wpd;
            destabilize w;
            if HM.mem stable x then (
              Option.may (add_infl x) orig;
              HM.remove called x;
            ) else (
              HM.replace stable x ();
              (iterate[@tailcall]) orig x
            )
          )
        and do_side y d = 
          let revive_suspended () =
            GobMutex.lock prom_mutex;
            List.iter (fun (top, z, sd) ->
                if tracing then trace "revive" "reviving %a" S.Var.pretty_trace z;
                promises := (Thread_pool.add_work pool (fun () -> solve_single top z sd))::!promises
              ) !suspended_vars;
            suspended_vars := [];
            GobMutex.unlock prom_mutex
          in
          (* TODO: Does doing it in this order somehow effect the locality of sides? *)
          Sides.add_side revive_suspended (y, d)
        and handle_side (y, v) =
          init rho y; (* necessary? *)
          let old_v = HM.find rho y in
          if S.Dom.leq v old_v then 
            ()
          else (
            let new_v = S.Dom.widen old_v (S.Dom.join old_v v) in
            if tracing then trace "side" "set side %a value: %a" S.Var.pretty_trace y S.Dom.pretty (S.Dom.widen v (S.Dom.join v old_v));
            HM.replace rho y (S.Dom.widen v (S.Dom.join v old_v));
            do_side y new_v;
            HM.replace stable y ();
            let w = HM.find_default infl y VS.empty in
            HM.replace infl y VS.empty;
            destabilize w
          )
        in

        (* begining of solve_single *)
        HM.replace stable x_poi ();
        HM.replace called x_poi ();
        iterate None x_poi;
        let suspend () =
          GobMutex.lock prom_mutex;
          if tracing then trace "suspend" "suspending %a" S.Var.pretty_trace x_poi;
          suspended_vars := (is_primary, x_poi, sd)::!suspended_vars;
          GobMutex.unlock prom_mutex
        in
        let rec wait () =
          match Sides.updates_or_fin suspend !obs with
          | Sides.NewSide -> (
              obs := Sides.process_updates !obs handle_side;
              if HM.mem stable x_poi then
                wait ()
              else
                HM.replace stable x_poi ();
              HM.replace called x_poi ();
              iterate None x_poi;
              wait ()
            ) 
          | Sides.Fin -> ()
        in
        wait ()
      in

      (* Imperative part starts here*)
      let init_data = create_empty_data () in

      let set_start (x,d) =
        if tracing then trace "sol2" "set_start %a ## %a" S.Var.pretty_trace x S.Dom.pretty d;
        init init_data.rho x;
        HM.replace init_data.rho x d;
        HM.replace init_data.stable x ();
      in

      start_event ();

      List.iter set_start st;

      List.iter (init init_data.rho) vs;
      (* If we have multiple start variables vs, we might solve v1, then while solving v2 we side some global which v1 depends on with a new value. Then v1 is no longer stable and we have to solve it again. *)
      let i = ref 0 in
      let rec solver () = (* as while loop in paper *)
        incr i;
        let unstable_vs = List.filter (neg (HM.mem init_data.stable)) vs in
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
                  solve_single true x init_data; 
                  Thread_pool.await_all pool (!promises)
                )
            ) unstable_vs;
          solver ();
        )
      in
      solver ();
      Thread_pool.finished_with pool;
      (* Before we solved all unstable vars in rho with a rhs in a loop. This is unneeded overhead since it also solved unreachable vars (reachability only removes those from rho further down). *)
      (* After termination, only those variables are stable which are
       * - reachable from any of the queried variables vs, or
       * - effected by side-effects and have no constraints on their own (this should be the case for all of our analyses). *)

      stop_event ();
      (*print_data_verbose data "Data after iterate completed";

        if GobConfig.get_bool "dbg.print_wpoints" then (
        Logs.newline ();
        Logs.debug "Widening points:";
        HM.iter (fun k () -> Logs.debug "%a" S.Var.pretty_trace k) wpoint;
        Logs.newline ();
        );


        print_data_verbose data "Data after postsolve";*)

      (* TODO: make a better merge here*)
      let final_rho = List.fold (fun acc (_,_,sd) -> 
          HM.merge (
            fun k ao bo -> 
              match ao, bo with
              | None, None -> None
              | Some a, None -> ao
              | None, Some b -> bo
              | Some a, Some b -> if S.Dom.equal a b then ao 
                else (if tracing then trace "dbg_para" "Inconsistent data for %a:\n left: %a\n right: %a" S.Var.pretty_trace k S.Dom.pretty a S.Dom.pretty b; Some (S.Dom.join a b)) 
          ) acc sd.rho
        ) (HM.create 10) !suspended_vars in
      if tracing then trace "dbg_para" "final_rho len: %d" (HM.length final_rho);
      final_rho
  end

let () =
  Selector.add_creating_eq_solver ("td_parallel_dist", (module PostSolver.CreatingEqIncrSolverFromCreatingEqSolver (Base)));
