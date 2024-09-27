(** Terminating, parallelized top-down solver with side effects. ([td_parallel_base]). TODO: better Name*)

(** Top down solver that is parallelized. TODO: better description *)
(* Options:
 * - solvers.td3.parallel_domains (default: 2): Maximal number of Domains that the solver can use in parallel.
 * TODO: support 'solvers.td3.remove-wpoint' option? currently it acts as if this option was always enabled *)

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

    let job_id_counter = (Atomic.make 10)

    let solve st vs =
      let nr_threads = GobConfig.get_int "solvers.td3.parallel_domains" in
      let nr_threads = if nr_threads = 0 then (Cpu.numcores ()) else nr_threads in

      let pool = Thread_pool.create nr_threads in

      let data = create_empty_data ()
      in

      let infl = data.infl in
      let rho = data.rho in
      let wpoint = data.wpoint in
      let stable = data.stable in
      let called = data.called in
      let root = data.root in

      let () = print_solver_stats := fun () ->
          print_data data;
          Logs.info "|called|=%d" (LHM.length called);
          print_context_stats @@ LHM.to_hashtbl rho
      in

      let add_infl y x =
        if tracing then trace "infl" "add_infl %a %a" S.Var.pretty_trace y S.Var.pretty_trace x;
        LHM.replace infl y (VS.add x (LHM.find_default infl y VS.empty));
      in

      let init x =
        if not (LHM.mem rho x) then (
          if tracing then trace "init" "init %a" S.Var.pretty_trace x;
          new_var_event x;
          LHM.replace rho x (S.Dom.bot ())
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
            LHM.lock y rho;
            let was_stable = LHM.mem stable y in
            if tracing && was_stable then trace "destab-v" "stable remove %a (root:%b, called:%b)" S.Var.pretty_trace y (LHM.mem root y) (LHM.mem called y);
            LHM.remove stable y;
            if was_stable && not (LHM.mem called y) then (
              (* destabilize infl *)
              let inner_w = LHM.find_default infl y VS.empty in
              LHM.replace infl y VS.empty;
              let is_root = LHM.mem root y in
              LHM.unlock y rho;
              if is_root then create_task prom y; (* This has to be y *)
              destabilize prom inner_w
            ) else (
              LHM.unlock y rho    
            )
          ) outer_w

      (** creates a task to solve for y *)
      and create_task outer_prom y =
        let job_id = Atomic.fetch_and_add job_id_counter 1 in
        let work_fun () =
          LHM.lock y rho;
          if LHM.mem called y then (
            LHM.unlock y rho;
          ) else (
            if tracing then trace "thread_pool" "starting task %d to iterate %a" job_id S.Var.pretty_trace y;
            LHM.replace called y ();
            LHM.replace stable y ();
            LHM.replace root y ();
            LHM.unlock y rho;
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
          if tracing then trace "sol_query" "%d entering query for %a; stable %b; called %b" job_id S.Var.pretty_trace y (LHM.mem stable y) (LHM.mem called y);
          LHM.lock y rho;
          get_var_event y;
          add_infl y x;
          if LHM.mem called y then (
            if tracing then trace "sol2" "query adding wpoint %a from %a" S.Var.pretty_trace y S.Var.pretty_trace x;
            LHM.replace wpoint y (); 
          ) else if not (LHM.mem stable y) then (
            if S.system y = None then (
              init y;
              LHM.replace stable y ()
            ) else (
              LHM.replace called y ();
              LHM.replace stable y ();
              LHM.unlock y rho;
              if tracing then trace "iter" "iterate called from query";
              iterate (Some x) prom y job_id;
              LHM.lock y rho
            )
          );
          let tmp = LHM.find rho y in
          LHM.unlock y rho;
          if tracing then trace "sol_query" " %d exiting query for %a" job_id S.Var.pretty_trace y;
          if tracing then trace "answer" "answer: %a" S.Dom.pretty tmp;
          tmp
        in

        let side x y d = (* side from x to y; only to variables y w/o rhs; x only used for trace *)
          if tracing then trace "side" "%d side to %a from %a" job_id S.Var.pretty_trace y S.Var.pretty_trace x;
          if tracing then trace "side-v" "%d side to %a (wpx: %b) from %a ## value: %a" job_id S.Var.pretty_trace y (LHM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty d;
          assert (S.system y = None);
          LHM.lock y rho;
          init y;
          let old = LHM.find rho y in
          if S.Dom.leq d old then (
            LHM.unlock y rho
          ) else (
            let widen a b =                 
              if M.tracing then M.trace "sidew" "%d side widen %a" job_id S.Var.pretty_trace y;
              S.Dom.widen a (S.Dom.join a b)    
            in 
            if tracing then trace "update" "%d side update %a with \n\t%a" job_id S.Var.pretty_trace x S.Dom.pretty (widen old d);
            LHM.replace rho y (widen old d);
            LHM.replace stable y ();
            let w = LHM.find_default infl y VS.empty in
            LHM.replace infl y VS.empty;
            LHM.unlock y rho;
            if tracing then trace "destab" "%d side destabilizing %a" job_id S.Var.pretty_trace y;
            destabilize prom w
          )
        in

        let create x y = (* create called from x on y *)
          if tracing then trace "create" "create from td_parallel_base was executed from %a on %a (called:%b)" S.Var.pretty_trace x S.Var.pretty_trace y (LHM.mem called y);
          create_task prom y
        in

        (* begining of iterate*)
        if tracing then trace "iter" "%d iterate %a, stable: %b, wpoint: %b" job_id S.Var.pretty_trace x (LHM.mem stable x) (LHM.mem wpoint x);
        LHM.lock x rho;
        init x;
        assert (S.system x <> None);
        let wp = LHM.mem wpoint x in (* if x becomes a wpoint during eq, checking this will delay widening until next iterate *)
        LHM.unlock x rho;
        if tracing then trace "eq" "eval eq for %a" S.Var.pretty_trace x;
        let eqd = eq x (query x) (side x) (create x) in
        LHM.lock x rho;
        let old = LHM.find rho x in
        let wpd = (* d after box operator (if wp) *)
          if not wp then 
            eqd
          else (if tracing then trace "wpoint" "box widening %a" S.Var.pretty_trace x; box old eqd)
        in
        (* TODO: wrap S.Dom.equal in timing if a reasonable threadsafe timing becomes available *)
        if S.Dom.equal wpd old then (
          (* old = wpd*)
          if LHM.mem stable x then (
            Option.may (add_infl x) orig;
            LHM.remove called x;
            LHM.remove wpoint x;
            LHM.unlock x rho
          ) else (
            LHM.replace stable x ();
            LHM.unlock x rho;
            if tracing then trace "iter" "iterate still unstable %a" S.Var.pretty_trace x;
            (iterate[@tailcall]) orig prom x job_id
          )
        ) else (
          (* old != wpd*)
          if tracing then trace "update" "%d iterate update %a with \n\t%a" job_id S.Var.pretty_trace x S.Dom.pretty wpd;
          LHM.replace rho x wpd;
          let w = LHM.find_default infl x VS.empty in
          LHM.replace infl x VS.empty;
          LHM.unlock x rho;
          if tracing then trace "destab" "%d iterate destabilizing %a" job_id S.Var.pretty_trace x;
          destabilize prom w;
          LHM.lock x rho;
          if LHM.mem stable x then (
            Option.may (add_infl x) orig;
            LHM.remove called x;
            LHM.unlock x rho;
          ) else (
            LHM.replace stable x ();
            LHM.unlock x rho;
            if tracing then trace "iter" "iterate changed %a" S.Var.pretty_trace x;
            (iterate[@tailcall]) orig prom x job_id
          )
        )
      in

      let set_start (x,d) =
        init x;
        LHM.replace rho x d;
        LHM.replace stable x ();
      in

      (* beginning of main solve *)
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

      if GobConfig.get_bool "dbg.print_wpoints" then (
        Logs.newline ();
        Logs.debug "Widening points:";
        LHM.iter (fun k () -> Logs.debug "%a" S.Var.pretty_trace k) wpoint;
        Logs.newline ();
      );

      LHM.to_hashtbl rho
  end

let () =
  Selector.add_creating_eq_solver ("td_parallel_base",  (module PostSolver.CreatingEqIncrSolverFromCreatingEqSolver (Base)));
