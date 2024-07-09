(** Terminating, parallelized top-down solver with side effects ([td_parallel_stealing]).

    @see <https://doi.org/10.1017/S0960129521000499> Seidl, H., Vogler, R. Three improvements to the top-down solver.
    @see <https://arxiv.org/abs/2209.10445> Interactive Abstract Interpretation: Reanalyzing Whole Programs for Cheap. *)

(** Terminating top down solver that is parallelized for some cases, where multiple unknowns have to be solved for a rhs. *)
(* TD3: see paper 'Three Improvements to the Top-Down Solver' https://dl.acm.org/doi/10.1145/3236950.3236967
 * Option solvers.td3.* (default) ? true : false (solver in paper):
 * - term (true) ? use phases for widen+narrow (TDside) : use box (TDwarrow)*)

open Batteries
open ConstrSys
open Messages

(* parameters - TODO: change to goblint options *)
let lowest_prio = 10
let highest_prio = 0
let nr_threads = 1
let map_size = 1000

module M = Messages

module LockableHashtbl (H:Hashtbl.HashedType) (HM:Hashtbl.S with type key = H.t) = 
struct
  type key = HM.key
  type 'a t = (Dmutex.t * 'a HM.t) array

  (* double hash to make sure that not all elements in a top-level bucket have the same hash*)
  let bucket_index lhm k = Int.abs ((Hashtbl.hash (H.hash k)) mod (Array.length lhm))

  let create sz = Array.init sz (fun _ -> Dmutex.create (), HM.create 10)

  let length lhm = Array.fold (fun l (_, hm) -> l + (HM.length hm)) 0 lhm

  let find lhm k = 
    let i = bucket_index lhm k in
    let (_, hm) = Array.get lhm i in
    HM.find hm k

  let find_default lhm k d =
    let i = bucket_index lhm k in
    let (_, hm) = Array.get lhm i in
    HM.find_default hm k d

  let remove lhm k = 
    let i = bucket_index lhm k in
    let (_, hm) as ele = Array.get lhm i in
    HM.remove hm k;
    Array.set lhm i ele

  let replace lhm k v =
    let i = bucket_index lhm k in
    let (_, hm) as ele = Array.get lhm i in
    HM.replace hm k v;
    Array.set lhm i ele

  let mem lhm k = 
    let i = bucket_index lhm k in
    let (_, hm) = Array.get lhm i in
    HM.mem hm k

  let iter f lhm = Array.iter (fun (_, hm) -> HM.iter f hm) lhm
  let iter_safe f lhm = Array.iter (fun (me, hm) -> Dmutex.lock me; HM.iter f hm; Dmutex.unlock me) lhm

  (*
  let to_hashtbl lhm = 
    let mapping_list = Array.fold (fun acc (_, hm) -> List.append acc (HM.to_list hm)) [] lhm in
    HM.of_list mapping_list
  *)

  let to_hashtbl lhm = Array.fold (fun acc (_, hm) ->
      HM.merge (
        fun k ao bo -> 
          match ao, bo with
          | None, None -> None
          | Some a, None -> ao
          | None, Some b -> bo
          | Some a, Some b -> failwith "LockableHashtbl: One key present in multiple buckets"
      ) acc hm) (HM.create 10) lhm

  let lock k lhm = 
    if tracing then trace "lock" "locking %d" (H.hash k);
    let (me, _) = Array.get lhm (bucket_index lhm k) in
    Dmutex.lock me

  let unlock k lhm = 
    if tracing then trace "lock" "unlocking %d" (H.hash k);
    let (me, _) = Array.get lhm (bucket_index lhm k) in
    Dmutex.unlock me
end 

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

    let create_empty_thread_data () = 
      {
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
      let data = create_empty_solver_data ()
      in

      let rho = data.rho in
      let stable = data.stable in
      let called = data.called in

      let () = print_solver_stats := fun () ->
          print_data data;
          print_context_stats @@ LHM.to_hashtbl rho
      in

      let init x =
        if tracing then trace "sol2" "init %a" S.Var.pretty_trace x;
        if not (LHM.mem rho x) then (
          new_var_event x;
          LHM.replace rho x (S.Dom.bot ())
        )
      in

      let eq x get set =
        if tracing then trace "sol2" "eq %a" S.Var.pretty_trace x;
        match S.system x with
        | None -> S.Dom.bot ()
        | Some f -> f get set
      in

      let solve_thread x thread_id =
        (* init thread local data *)
        let t_data = create_empty_thread_data ()
        in
        let wpoint = t_data.wpoint in
        let infl = t_data.infl in
        let prio = (lowest_prio - thread_id - 1) in

        (* returns the current called/stable prio value of x. returns lowest_prio if not present *)
        let stable_prio x = LHM.find_default stable x lowest_prio in
        let called_prio x = LHM.find_default called x lowest_prio in

        let add_infl y x =
          if tracing then trace "infl" "add_infl %a %a" S.Var.pretty_trace y S.Var.pretty_trace x;
          HM.replace infl y (VS.add x (try HM.find infl y with Not_found -> VS.empty));
        in

        let rec destabilize x =
          if tracing then trace "sol2" "destabilize %a" S.Var.pretty_trace x;
          let w = HM.find_default infl x VS.empty in
          HM.replace infl x VS.empty;
          VS.iter (fun y ->
              if tracing then trace "lock" "locking %a in destab" S.Var.pretty_trace y;
              LHM.lock y rho;
              if (prio <= stable_prio y) then (
                if tracing then trace "sol2" "stable remove %a" S.Var.pretty_trace y;
                if tracing then trace "destab" "destabilizing %a" S.Var.pretty_trace y;
                LHM.replace stable y lowest_prio
              );
              if tracing then trace "lock" "unlocking %a in destab" S.Var.pretty_trace y;
              LHM.unlock y rho;
              destabilize y
            ) w
        in

        let rec iterate ?reuse_eq x = (* ~(inner) solve in td3*)
          let query x y = (* ~eval in td3 *)
            if tracing then trace "called" "entering query with prio %d for %a" (called_prio y) S.Var.pretty_trace y;
            if tracing then trace "lock" "locking %a in query" S.Var.pretty_trace y;
            LHM.lock y rho;
            if tracing then trace "sol2" "query %a ## %a" S.Var.pretty_trace x S.Var.pretty_trace y;
            get_var_event y;
            if not (called_prio y <= prio) then (
              (* TODO (see td-parallel repo): check if necessary/enough *)
              (* If owning new, make sure it is not in point *)
              if tracing then trace "wpoint" "query removing wpoint %a (%b)" S.Var.pretty_trace y (HM.mem wpoint y);
              if HM.mem wpoint y then HM.remove wpoint y;
              if tracing then trace "sol2" "simple_solve %a (rhs: %b)" S.Var.pretty_trace y (S.system y <> None);
              if S.system y = None then (
                init y;
                LHM.replace stable y prio
              ) else (
                if tracing then trace "called" "query setting prio from %d to %d for %a" (called_prio y) prio S.Var.pretty_trace y;
                LHM.replace called y prio;
                if tracing then trace "lock" "unlocking %a in query" S.Var.pretty_trace y;
                LHM.unlock y rho;
                (* call iterate unlocked *)
                if tracing then trace "iter" "iterate called from query";
                iterate y;
                if tracing then trace "lock" "locking %a in query 2" S.Var.pretty_trace y;
                LHM.lock y rho;
                if (called_prio y >= prio) then 
                  if tracing then trace "called" "query setting prio back from %d to %d for %a" (called_prio y) lowest_prio S.Var.pretty_trace y; 
                LHM.replace called y lowest_prio
              )
            ) else (
              if tracing then trace "wpoint" "query adding wpoint %a (%b)" S.Var.pretty_trace y (HM.mem wpoint y);
              HM.replace wpoint y ()
            );
            let tmp = LHM.find rho y in
            if tracing then trace "lock" "unlocking %a in query 2" S.Var.pretty_trace y;
            LHM.unlock y rho;
            add_infl y x;
            if tracing then trace "sol2" "query %a ## %a -> %a" S.Var.pretty_trace x S.Var.pretty_trace y S.Dom.pretty tmp;
            if tracing then trace "called" "exiting query for %a" S.Var.pretty_trace y;
            tmp
          in

          let side x y d = (* side from x to y; only to variables y w/o rhs; x only used for trace *)
            if tracing then trace "sol2" "side to %a (wpx: %b) from %a ## value: %a" S.Var.pretty_trace y (HM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty d;
            if S.system y <> None then (
              Logs.warn "side-effect to unknown w/ rhs: %a, contrib: %a" S.Var.pretty_trace y S.Dom.pretty d;
            );
            assert (S.system y = None);
            init y;

            (* begining of side *)
            (* TODO check if this works *)
            if tracing then trace "lock" "locking %a in side" S.Var.pretty_trace y;
            LHM.lock y rho;
            if (called_prio y >= prio) then (
              if tracing then trace "called" "side setting prio from %d to %d for %a" (called_prio y) prio S.Var.pretty_trace y;
              LHM.replace called y prio;
              let old = LHM.find rho y in
              (* currently any side-effect after the first one will be widened *)
              let widen a b = 
                if M.tracing then M.trace "sidew" "side widen %a" S.Var.pretty_trace y;
                S.Dom.widen a (S.Dom.join a b)    
              in 
              let tmp = if HM.mem wpoint y then widen old d else S.Dom.join old d in
              if tracing then trace "lock" "unlocking %a in side" S.Var.pretty_trace y;
              if not (S.Dom.leq d old) then (
                LHM.replace rho y tmp;
                LHM.unlock y rho;
                destabilize y;
                if tracing then trace "wpoint" "side adding wpoint %a (%b)" S.Var.pretty_trace y (HM.mem wpoint y);
                HM.replace wpoint y ()
              ) else (
                LHM.unlock y rho
              )
            ) else (
              if tracing then trace "lock" "unlocking %a in side" S.Var.pretty_trace y;
              LHM.unlock y rho
            )
          in


          (* begining of iterate *)
          if tracing then trace "lock" "locking %a in iterate" S.Var.pretty_trace x;
          LHM.lock x rho;
          if tracing then trace "iter" "iterate %a, called: %b, stable: %b, wpoint: %b" S.Var.pretty_trace x (called_prio x <= prio) (stable_prio x <= prio) (HM.mem wpoint x);
          init x;
          assert (S.system x <> None);
          if not (stable_prio x <= prio) then (
            if tracing then trace "sol2" "stable add %a" S.Var.pretty_trace x;
            LHM.replace stable x prio;
            (* Here we cache LHM.mem wpoint x before eq. If during eq evaluation makes x wpoint, then be still don't apply widening the first time, but just overwrite.
               It means that the first iteration at wpoint is still precise.
               This doesn't matter during normal solving (?), because old would be bot.
               This matters during incremental loading, when wpoints have been removed (or not marshaled) and are redetected.
               Then the previous local wpoint value is discarded automagically and not joined/widened, providing limited restarting of local wpoints. (See query for more complete restarting.) *)
            let wp = HM.mem wpoint x in (* if x becomes a wpoint during eq, checking this will delay widening until next iterate *)
            if tracing then trace "lock" "unlocking %a in iterate" S.Var.pretty_trace x;
            LHM.unlock x rho;
            let eqd = eq x (query x) (side x) in
            if tracing then trace "lock" "locking %a in iterate 2" S.Var.pretty_trace x;
            LHM.lock x rho;
            let old = LHM.find rho x in (* d from older iterate *) (* find old value after eq since wpoint restarting in eq/query might have changed it meanwhile *)
            let wpd = (* d after box operator (if wp) *)
              if not wp then 
                eqd
              else (if tracing then trace "wpoint" "box widening %a" S.Var.pretty_trace x; box old eqd)
            in
            if tracing then trace "sol" "Var: %a (wp: %b)\nOld value: %a\nEqd: %a\nNew value: %a" S.Var.pretty_trace x wp S.Dom.pretty old S.Dom.pretty eqd S.Dom.pretty wpd;
            if not (Timing.wrap "S.Dom.equal" (fun () -> S.Dom.equal old wpd) ()) then ( 
              (* old != wpd *)
              if (stable_prio x >= prio && called_prio x >= prio) then (
                if tracing then trace "sol" "Changed";
                update_var_event x old wpd;
                LHM.replace  rho x wpd;
                if tracing then trace "lock" "unlocking %a in iterate 2" S.Var.pretty_trace x;
                LHM.unlock x rho;
                destabilize x;
                if tracing then trace "iter" "iterate changed";
                (iterate[@tailcall]) x
              ) else (
                LHM.unlock x rho
              )
            ) else (
              (* old = wpd*)
              if not (stable_prio x <= prio) then (
                if tracing then trace "iter" "iterate still unstable";
                LHM.unlock x rho;
                (iterate[@tailcall]) x
              ) else (
                if tracing then trace "wpoint" "iterate removing wpoint %a (%b)" S.Var.pretty_trace x (HM.mem wpoint x);
                HM.remove wpoint x;
                if tracing then trace "lock" "unlocking %a in iterate 2" S.Var.pretty_trace x;
                LHM.unlock x rho
              )
            )
          ) else (
            if tracing then trace "lock" "unlocking %a in iterate" S.Var.pretty_trace x;
            LHM.unlock x rho;
          )
        in
        iterate x;
      in

      let set_start (x,d) =
        if tracing then trace "sol2" "set_start %a ## %a" S.Var.pretty_trace x S.Dom.pretty d;
        init x;
        LHM.replace rho x d;
        LHM.replace stable x highest_prio;
        (* iterate x Widen *)
      in

      let start_threads x =
        let threads = Array.init nr_threads (fun j ->
            Thread.create (fun () -> solve_thread x j) ()) in 
        Array.iter Thread.join threads
      in

      (* Imperative part starts here*)
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
          List.iter (fun x -> (*LHM.replace called x highest_prio;*)
              if tracing then trace "multivar" "solving for %a" S.Var.pretty_trace x;
              start_threads x;
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

      LHM.to_hashtbl rho
  end

let () =
  Selector.add_solver ("td_parallel_stealing", (module PostSolver.EqIncrSolverFromEqSolver (Base)));
