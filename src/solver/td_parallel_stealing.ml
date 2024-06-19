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
    let (me, _) = Array.get lhm (bucket_index lhm k) in
    Dmutex.lock me

  let unlock k lhm = 
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
      called: int HM.t;
      stable: int HM.t;
    }

    type thread_data = {
      wpoint: unit HM.t;
      infl: VS.t HM.t;
    }

    let create_empty_solver_data () = {
      rho = LHM.create map_size;
      called = HM.create 10;
      stable = HM.create 10;
    }

    let create_empty_thread_data () = {
      wpoint = HM.create 10;
      infl = HM.create 10;
    }

    let print_data data =
      Logs.debug "|rho|=%d" (LHM.length data.rho);
      Logs.debug "|called|=%d" (HM.length data.called);
      Logs.debug "|stable|=%d" (HM.length data.stable)

    let print_data_verbose data str =
      if Logs.Level.should_log Debug then (
        Logs.debug "%s:" str;
        print_data data
      )

    type phase = Widen | Narrow [@@deriving show] (* used in iterate *)

    module CurrentVarS = ConstrSys.CurrentVarEqConstrSys (S)
    module S = CurrentVarS.S

    let solve st vs =
      let data = create_empty_solver_data ()
      in
      let t_data = create_empty_thread_data ()
      in

      let term  = GobConfig.get_bool "solvers.td3.term" in

      let rho = data.rho in
      let stable = data.stable in
      let called = data.called in

      let wpoint = t_data.wpoint in
      let infl = t_data.infl in

      let remove_wpoint = GobConfig.get_bool "solvers.td3.remove-wpoint" in

      let () = print_solver_stats := fun () ->
          print_data data;
          print_context_stats @@ LHM.to_hashtbl rho
      in

      let add_infl y x =
        if tracing then trace "sol2" "add_infl %a %a" S.Var.pretty_trace y S.Var.pretty_trace x;
        HM.replace infl y (VS.add x (try HM.find infl y with Not_found -> VS.empty));
      in

      let init x =
        if tracing then trace "sol2" "init %a" S.Var.pretty_trace x;
        if not (LHM.mem rho x) then (
          new_var_event x;
          LHM.replace rho x (S.Dom.bot ());
          HM.replace stable x lowest_prio;
          HM.replace called x lowest_prio;
        )
      in

      let eq x get set =
        if tracing then trace "sol2" "eq %a" S.Var.pretty_trace x;
        match S.system x with
        | None -> S.Dom.bot ()
        | Some f -> f get set
      in

      let rec destabilize x =
        if tracing then trace "sol2" "destabilize %a" S.Var.pretty_trace x;
        let w = HM.find_default infl x VS.empty in
        HM.replace infl x VS.empty;
        VS.iter (fun y ->
            if tracing then trace "sol2" "stable remove %a" S.Var.pretty_trace y;
            HM.replace stable y lowest_prio;
            if not (HM.find_default called y lowest_prio = highest_prio) then destabilize y
          ) w
      in

      let rec iterate ?reuse_eq x phase = (* ~(inner) solve in td3*)
        let own_prio = highest_prio in (* placeholder for singlethreaded *)
        let query x y = (* ~eval in td3 *)
          let simple_solve y =
            if tracing then trace "sol2" "simple_solve %a (rhs: %b)" S.Var.pretty_trace y (S.system y <> None);
            if S.system y = None then (
              init y;
              HM.replace stable y own_prio
            ) else (
              HM.replace called x own_prio;
              iterate y Widen;
              HM.replace called x lowest_prio)
          in
          if tracing then trace "sol2" "query %a ## %a" S.Var.pretty_trace x S.Var.pretty_trace y;
          get_var_event y;
          if not (HM.find_default called y lowest_prio = highest_prio) then (
            simple_solve y
          ) else (
            if tracing then trace "sol2" "query adding wpoint %a from %a" S.Var.pretty_trace y S.Var.pretty_trace x;
            HM.replace wpoint y ();
          );
          let tmp = LHM.find rho y in
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
          let old = LHM.find rho y in
          let tmp = op old d in
          if tracing then trace "sol2" "stable add %a" S.Var.pretty_trace y;
          HM.replace stable y own_prio;
          if not (S.Dom.leq tmp old) then (
            if tracing && not (S.Dom.is_bot old) then trace "solside" "side to %a (wpx: %b) from %a: %a -> %a" S.Var.pretty_trace y (HM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty old S.Dom.pretty tmp;
            if tracing && not (S.Dom.is_bot old) then trace "solchange" "side to %a (wpx: %b) from %a: %a" S.Var.pretty_trace y (HM.mem wpoint y) S.Var.pretty_trace x S.Dom.pretty_diff (tmp, old);
            (* LHM.replace rho y ((if LHM.mem wpoint y then S.Dom.widen old else identity) (S.Dom.join old d)); *)
            LHM.replace rho y tmp;
            destabilize y;
            (* make y a widening point. This will only matter for the next side _ y.  *)
            if tracing then trace "sol2" "side adding wpoint %a from %a" S.Var.pretty_trace y S.Var.pretty_trace x;
            HM.replace wpoint y ()
          )
        in  

        (* begining of iterate*)
        if tracing then trace "sol2" "iterate %a, phase: %s, called: %b, stable: %b, wpoint: %b" S.Var.pretty_trace x (show_phase phase) (HM.find_default called x lowest_prio = highest_prio) (HM.find_default stable x lowest_prio = highest_prio) (HM.mem wpoint x);
        init x;
        assert (S.system x <> None);
        if not (HM.find_default stable x lowest_prio = highest_prio) then (
          if tracing then trace "sol2" "stable add %a" S.Var.pretty_trace x;
          HM.replace stable x own_prio;
          (* Here we cache LHM.mem wpoint x before eq. If during eq evaluation makes x wpoint, then be still don't apply widening the first time, but just overwrite.
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
            | _ -> eq x (query x) (side x)
          in
          let old = LHM.find rho x in (* d from older iterate *) (* find old value after eq since wpoint restarting in eq/query might have changed it meanwhile *)
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
            (* if tracing && not (S.Dom.is_bot old) && LHM.mem wpoint x then trace "solchange" "%a (wpx: %b): %a -> %a" S.Var.pretty_trace x (LHM.mem wpoint x) S.Dom.pretty old S.Dom.pretty wpd; *)
            if tracing && not (S.Dom.is_bot old) && HM.mem wpoint x then trace "solchange" "%a (wpx: %b): %a" S.Var.pretty_trace x (HM.mem wpoint x) S.Dom.pretty_diff (wpd, old);
            update_var_event x old wpd;
            LHM.replace  rho x wpd;
            destabilize x;
            (iterate[@tailcall]) x phase
          ) else (
            (* TODO: why non-equal and non-stable checks in switched order compared to TD3 paper? *)
            if not (HM.find_default stable x lowest_prio = highest_prio) then ( (* value unchanged, but not stable, i.e. destabilized itself during rhs *)
              if tracing then trace "sol2" "iterate still unstable %a" S.Var.pretty_trace x;
              (iterate[@tailcall]) x Widen
            ) else (
              if term && phase = Widen && HM.mem wpoint x then ( (* TODO: or use wp? *)
                if tracing then trace "sol2" "iterate switching to narrow %a" S.Var.pretty_trace x;
                if tracing then trace "sol2" "stable remove %a" S.Var.pretty_trace x;
                HM.replace stable x lowest_prio;
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
        LHM.replace rho x d;
        HM.replace stable x highest_prio;
        (* iterate x Widen *)
      in

      let start_threads x =
        iterate x Widen;
      in

      (* Imperative part starts here*)
      start_event ();

      List.iter set_start st;

      List.iter init vs;
      (* If we have multiple start variables vs, we might solve v1, then while solving v2 we side some global which v1 depends on with a new value. Then v1 is no longer stable and we have to solve it again. *)
      let i = ref 0 in
      let rec solver () = (* as while loop in paper *)
        incr i;
        let unstable_vs = List.filter (neg (fun x -> HM.find_default stable x lowest_prio = highest_prio)) vs in
        if unstable_vs <> [] then (
          if Logs.Level.should_log Debug then (
            if !i = 1 then Logs.newline ();
            Logs.debug "Unstable solver start vars in %d. phase:" !i;
            List.iter (fun v -> Logs.debug "\t%a" S.Var.pretty_trace v) unstable_vs;
            Logs.newline ();
            flush_all ();
          );
          List.iter (fun x -> HM.replace called x highest_prio;
                      start_threads x;
                      HM.replace called x lowest_prio) unstable_vs;
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

      LHM.to_hashtbl rho
  end

let () =
  Selector.add_solver ("td_parallel_stealing", (module PostSolver.EqIncrSolverFromEqSolver (Base)));
