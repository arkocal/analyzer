(** Terminating, parallelized top-down solver with side effects. ([td_parallel_dist]).*)

(** Top down solver that is parallelized. TODO: better description *)
(* Options:
 * - solvers.td3.parallel_domains (default: 0 - automatic selection): Maximal number of Domains that the thread-pool of the solver can use in parallel.
 * TODO: support 'solvers.td3.remove-wpoint' option? currently it acts as if this option was always enabled *)

open Batteries
open ConstrSys
open Messages
open Goblint_parallel
open ParallelStats
open Messages

module M = Messages

module Base : GenericCreatingEqSolver =
  functor (S:CreatingEqConstrSys) ->
  functor (HM:Hashtbl.S with type key = S.v) ->
  struct
    open SolverBox.Warrow (S.Dom)
    (* TODO: Maybe different solver stats according for CreatingEQsys is needed *)
    (* include Generic.SolverStats (EqConstrSysFromCreatingEqConstrSys (S)) (HM) *)

    module VS = Set.Make (S.Var)
    module Thread_pool = Threadpool.Thread_pool

    open ParallelSolverStats (EqConstrSysFromCreatingEqConstrSys (S)) (HM)


    (* The following contains a possible refactoring of the sides module as a general update queue *)

    (* These are theoretically not sides but updates in general: For writing: formalize required properties *)
    (* TODO: This could be instance based, with a type t, and create: unit -> t *)
    (* module type Updates = sig *)
    (*   type bookmark *)
    (*   type entry *)
    (**)
    (*   (** [produce ~callback entry] adds [entry] to the list of updates.  *)
         (*       [callback] is called after the update is added. *) *)
    (*   val produce: callback:(unit -> unit) -> entry -> unit *)
    (*   (** [consume bookmark ~consumer] consumes all updates that were produced  *)
         (*       after [bookmark], and returns a new bookmark marking the last consumed update. *) *)
    (*   val consume: bookmark -> consumer:(entry -> unit) -> bookmark *)
    (*   (** [start_bookmark ()] returns a bookmark that marks the beginning of updates. *) *)
    (*   val start_bookmark: unit -> bookmark *)
    (**)
    (*   (** [finish bookmark ~finalizer] finalize if no updates are left. *) *)
    (*   val finish: bookmark -> finalizer:(unit -> unit) -> bool *)
    (* end *)
    (**)
    (* (* module SideUpdates: Updates = struct *) *)
    (* module SideUpdates = struct *)
    (*   type bookmark = int *)
    (*   type 'a t = { *)
    (*     mutable next_obs_index: bookmark; *)
    (*     mutex: GobMutex.t; *)
    (*     mutable updates: (bookmark*'a) list; *)
    (*   } *)
    (*   type entry = S.Var.t * S.Dom.t *)
    (*   type list_entry = bookmark * entry *)
    (**)
    (*   let next_obs_index = ref 1 *)
    (*   let (updates : list_entry list ref) = ref [] *)
    (**)
    (*   let start_bookmark () = 0 *)
    (**)
    (*   let mutex = GobMutex.create () *)
    (**)
    (*   let produce redo_prelim ((v,_) as side) = *)
    (*     GobMutex.lock mutex; *)
    (*     (* if tracing then trace "side_add" "%d adding side to %a (obs index: %d)" thread_id S.Var.pretty_trace v !next_obs_index; *) *)
    (*     updates := (!next_obs_index, side) :: !updates; *)
    (*     incr next_obs_index; *)
    (*     redo_prelim (); *)
    (*     GobMutex.unlock mutex *)
    (**)
    (*   (* Process all existing updates via f *) *)
    (*   (* id is the thread ID *) *)
    (*   (* Returns the new observation index *) *)
    (*   let consume last_obs_index f = *)
    (*     GobMutex.lock mutex; *)
    (*     let current_sides = !updates in *)
    (*     let new_bookmark = !next_obs_index-1 in *)
    (*     GobMutex.unlock mutex; *)
    (*     Seq.of_list current_sides  *)
    (*       |> Seq.take_while (fun (o,_) -> o <> last_obs_index)  *)
    (*       |> Seq.iter (fun (_, side) -> f side); *)
    (*     (* if tracing then trace "process_update" "%d processed sides %d to %d" thread_id (last_obs_index) (obs'); *) *)
    (*     new_bookmark *)
    (**)
    (*   (* Returns if we have updates or if we are finished *) *)
    (*   (* Unknowns must be marked as preliminary, even if all updates are accounted for *) *)
    (*   (* since new updates might come later *) *)
    (*   let finish obs mark_prelim = *)
    (*     GobMutex.lock mutex; *)
    (*     if !next_obs_index-1 <> obs then *)
    (*       begin *)
    (*         GobMutex.unlock mutex;  *)
    (*         false *)
    (*       end *)
    (*     else *)
    (*       begin *)
    (*         mark_prelim (); *)
    (*         GobMutex.unlock mutex; *)
    (*         true *)
    (*       end *)
    (* end *)

    module type Sides = sig
      type obs_bookmark
      type remaining_status = NewSide | Fin
      type side_effect = S.Var.t * S.Dom.t

      val process_updates: int -> obs_bookmark -> (side_effect -> unit) -> obs_bookmark
      val add_side: int -> (unit -> unit) -> side_effect -> unit
      val no_observations: unit -> obs_bookmark

      val updates_or_fin: int -> obs_bookmark -> (unit -> unit) -> remaining_status
    end

    (* TODO: I am not sure if the data structure currently used is the best solution *)
    (* It is kind of difficult to manage the indices *)
    (* We could use a lock-free linked list and remember the element that was processed last *)
    (* An AtomicLinkedList is already defined in parallel_util.ml *)
    module Sides: Sides = struct
      type obs_bookmark = int
      type remaining_status = NewSide | Fin
      type side_effect = S.Var.t * S.Dom.t
      type side_entry = obs_bookmark * side_effect
      (* Index of the next observation  *)
      (* This is used to determine if we have processed all sides *)
      let next_obs_index = ref 1
      (* This is defined once module-wise, and not per solver thread *)
      let (sides : side_entry list ref) = ref []
      let no_observations () = 0

      let mutex = GobMutex.create ()

      (* Resolve is used to create threads for preliminary unknowns. *)
      let add_side thread_id resolve ((v,_) as side) =
        GobMutex.lock mutex;
        if tracing then trace "side_add" "%d adding side to %a (obs index: %d)" thread_id S.Var.pretty_trace v !next_obs_index;
        sides := (!next_obs_index, side) :: !sides;
        incr next_obs_index;
        resolve ();
        GobMutex.unlock mutex

      (* Process all existing updates via f *)
      (* id is the thread ID *)
      (* Returns the new observation index *)
      let process_updates thread_id last_obs_index f =
        GobMutex.lock mutex;
        let current_sides = !sides in
        let obs' = !next_obs_index-1 in
        GobMutex.unlock mutex;
        (* let rec process = function *)
        (*   | [] -> () *)
        (*   | (o,_)::_ when o = last_obs_index -> ()  (* Skip already processed sides at the end *) *)
        (*   | (_, side)::tl -> ( *)
        (*     f side;  *)
        (*     process tl *)
        (*   ) *)
        (* in *)
        (* process current_sides; *)
        (* same as above with seqs: *)
        (* TODO: ensure this is actually correct *)
        Seq.of_list current_sides 
        |> Seq.take_while (fun (o,_) -> o <> last_obs_index) 
        |> Seq.iter (fun (_, side) -> f side);
        if tracing then trace "process_update" "%d processed sides %d to %d" thread_id (last_obs_index) (obs');
        obs'

      (* Returns if we have updates or if we are finished *)
      (* Unknowns must be marked as preliminary, even if all updates are accounted for *)
      (* since new updates might come later *)
      (* Thread id is included for consistency with other functions *)
      let updates_or_fin thread_id obs mark_prelim =
        GobMutex.lock mutex;
        let result = if !next_obs_index-1 <> obs then NewSide 
          else (mark_prelim (); Fin) in
        GobMutex.unlock mutex;
        result
        (* if !next_obs_index-1 <> obs then *)
        (*   begin *)
        (*     GobMutex.unlock mutex;  *)
        (*     NewSide *)
        (*   end *)
        (* else *)
        (*   begin *)
        (*     mark_prelim (); *)
        (*     GobMutex.unlock mutex; *)
        (*     Fin *)
        (*   end *)
    end

    type unknown_data = {
      infl: VS.t;
      rho: S.Dom.t;
      wpoint: bool;
      stable: bool;
      called: bool
    }

    let create_unknown_data () = {
      infl = VS.empty;
      rho = S.Dom.bot ();
      wpoint = false;
      stable = false;
      called = false
    }

    type solver_data = {
      (* Sides.obs is an index! The actual observations are stored in the sides data structure *)
      (* Therefore, this obs is solver thread specific! *)
      (* TODO: obs_index should rather be called bookmark, bc. not necessarily an index *)
      obs_index: Sides.obs_bookmark ref;

      unknowns: unknown_data ref HM.t;
    }

    let create_empty_data () = {
      obs_index = ref (Sides.no_observations ());
      unknowns = HM.create 10;
    }

    let init (unknowns : unknown_data ref HM.t) x =
      let found_data = HM.find_option unknowns x in 
      match found_data with
      | Some data -> data
      | None -> 
        let data = ref @@ create_unknown_data () in
        if tracing then trace "init" "init %a" S.Var.pretty_trace x;
        (* TODO introduce ids here *)
        new_var_event 0 x;
        HM.replace unknowns x data;
        data

    let create_start_data st =
      let data = create_empty_data () in
      (* Start variables are provided as pairs of variable and value to the solver *)
      (* The following block brings the data into the format that the solver expects *)
      let set_start (x,d) =
        let new_ref = init data.unknowns x in
        new_ref := {!new_ref with rho = d; stable = true};
      in
      (* TODO: maybe it makes sense to use Stat.start_event in general *)
      (* Otherwise it suggests it is doing something in the solver *)
      start_event ();
      List.iter set_start st;
      data


    let print_data data =
      Logs.info "|unknowns|=%d" (HM.length data.unknowns)

    let print_data_verbose data str =
      if Logs.Level.should_log Debug then (
        Logs.debug "%s:" str;
        print_data data
      )

    let solve st vs =
      let nr_domains = GobConfig.get_int "solvers.td3.parallel_domains" in
      let nr_domains = if nr_domains = 0 then (Domain.recommended_domain_count ()) else nr_domains in

      (* domain with id 0 is always working. Threadpool initialized with n means (domain 0) + n additional domains are working *)
      let pool = Thread_pool.create (nr_domains - 1) in

      (* Promises keep track of the threads that are possibly still running *)
      (* We only add to this when processing create and reviving as of 2025.01.16 *)
      let promises = ref [] in
      (* TODO: Again, a thread safe data structure would be better *)
      (* Even if we end-up using mutexes, it is simpler to use *)
      let prom_mutex = GobMutex.create () in

      (* MOST LIKELY: this is used to keep track of variables, for which we have created a thread *)
      (* and that we might want to revive later, so no new thread should be created apart from through revival *)
      (* TODO: this is currently synced via prom_mutex, check if this is necessary/makes sense *)
      let created_vars = HM.create 10 in
      (* These are variables, whose threads are stopped, but not surely finally stable, as updates can come later *)
      (* TODO: this is currently synced via prom_mutex, check if this is necessary/makes sense *)
      let prelim_vars = ref [] in

      let job_id_counter = (Atomic.make 1) in

      (* TODO: make something reasonable out of this
         let () = print_solver_stats := fun () ->
          print_data data;
          Logs.info "|called|=%d" (HM.length called);
          print_context_stats rho
         in *)

      (* prepare start_rho and start_stable here to have it available to all tasks *)
      (* These are start points in the analzed code, such as the main main function *)
      (* TODO: using st, create start state and save it here *)
      let start_unknowns = HM.create 10 in


      (* Start variables are provided as pairs of variable and value to the solver *)
      (* The following block brings the data into the format that the solver expects *)
      let set_start (x,d) =
        let new_ref = init start_unknowns x in
        new_ref := {!new_ref with rho = d; stable = true};
      in
      (* TODO: maybe it makes sense to use Stat.start_event in general *)
      (* Otherwise it suggests it is doing something in the solver *)
      start_event ();
      List.iter set_start st;

      (** solves for a single point-of-interest variable (x_poi) *)
      (* primary means user is interested in the result *)
      let rec solve_single is_primary x_poi sd job_id =
        let obs = sd.obs_index in
        let unknowns = sd.unknowns in

        let add_infl y x =
          if tracing then trace "infl" "%d add %a influences %a" job_id S.Var.pretty_trace y S.Var.pretty_trace x;
          (* TODO back to find instead of init? *)
          (* let y_ref = HM.find unknowns y in *)
          let y_ref = init unknowns y in
          y_ref := {!y_ref with infl = VS.add x !y_ref.infl}
        in

        let eq x get set create =
          if tracing then trace "eq" "eq %a" S.Var.pretty_trace x;
          match S.system x with
          | None -> S.Dom.bot ()
          | Some f -> f get set (Some create)
        in

        let rec destabilize outer_w =
          VS.iter (fun y ->
              let y_ref = HM.find unknowns y in
              if not (!y_ref.stable) then
                ()
              else if !y_ref.called then (
                if tracing then trace "destab" "%d stable remove %a" job_id S.Var.pretty_trace y;
                y_ref := {!y_ref with stable = false};
              ) else (
                let inner_w = !y_ref.infl in
                if tracing then trace "destab" "%d stable remove %a" job_id S.Var.pretty_trace y;
                y_ref := {!y_ref with infl = VS.empty; stable = false};
                destabilize inner_w
              )
            ) outer_w
        in

        (** iterates to solve for x *)
        let rec iterate orig x = (* ~(inner) solve in td3*)
          let query x y = (* ~eval in td3 *)
            let y_ref = init unknowns y in
            if tracing then trace "sol_query" "%d query for %a from %a; stable %b; called %b" job_id S.Var.pretty_trace y S.Var.pretty_trace x (!y_ref.stable) (!y_ref.called);
            if !y_ref.stable || !y_ref.called then (
              if !y_ref.called then (y_ref := {!y_ref with wpoint = true});
              add_infl y x
            ) else (
              if S.system y = None then (
                (* init rho y; *) (* Should not be necessary, since it is searched/created at the beginning of the method *)
                y_ref := {!y_ref with stable = true};
                (* Normally, we process updates in iterate *)
                (* For vars without constraints, we need to handle sides here *)
                (* as they do not result in an iterate call *)
                obs := Sides.process_updates job_id !obs handle_side; 
                add_infl y x
              ) else (
                y_ref := {!y_ref with called = true; stable = true};
                iterate (Some x) y
                (* Infl will be added in iterate *)
              )
            );
            let tmp = !y_ref.rho in
            if tracing then trace "answer" "exiting query for %a\nanswer: %a" S.Var.pretty_trace y S.Dom.pretty tmp;
            tmp
          in

          let side x y d = (* side from x to y; only to variables y w/o rhs; x only used for trace *)
            let y_ref = init unknowns y in
            if tracing then trace "side" "%d side to %a (wpx: %b) from %a" job_id S.Var.pretty_trace y (!y_ref.wpoint) S.Var.pretty_trace x;
            (* Question: Note that globals are not included in rho yet! This happens in handle_side, also for the thread itself! *)
            add_side_to_struct y d
          in

          let create x y = (* create called from x on y *)
            if tracing then trace "create" "create from td_parallel_dist is being executed from %a on %a" S.Var.pretty_trace x S.Var.pretty_trace y;
            GobMutex.lock prom_mutex;
            if HM.mem created_vars y then
              ()
            else (
              HM.replace created_vars y ();
              (* Solve single does not create its data, but expects it to be created before the call *)
              (* At least st must be passed to create start data *)

              (* We can possibly reuse some data, but it is not happening yet as of 2015.01.16 *)
              (* let new_sd = {(create_empty_data ()) with unknowns = HM.copy start_unknowns} in *)
              let new_sd = create_start_data st in
              let new_id = Atomic.fetch_and_add job_id_counter 1 in
              if tracing then trace "thread_pool" "%d adding job %d to solve for %a(%d)" job_id new_id S.Var.pretty_trace y (S.Var.hash y);
              (* TODO: are all primaries surely started or at least added to created_vars before? *)
              promises := (Thread_pool.add_work pool (fun () -> solve_single false y new_sd new_id))::!promises
            );
            GobMutex.unlock prom_mutex
          in

          (* begining of iterate*)
          assert (S.system x <> None);
          let x_ref = init unknowns x in
          if tracing then trace "sol2" "iterate %a, called: %b, stable: %b, wpoint: %b" S.Var.pretty_trace x (!x_ref.called) (!x_ref.stable) (!x_ref.wpoint);
          let wp = !x_ref.wpoint in (* if x becomes a wpoint during eq, checking this will delay widening until next iterate *)
          let eqd = eq x (query x) (side x) (create x) in
          (* Process updates before! every rhs and save new index *)
          obs := Sides.process_updates job_id !obs handle_side;
          let old = !x_ref.rho in
          let wpd = (* d after widen/narrow (if wp) *)
            if not wp then eqd
            else box old eqd
          in
          (* TODO: wrap S.Dom.equal in timing if a reasonable threadsafe timing becomes available *)
          if S.Dom.equal old wpd then (
            (* old = wpd*)
            if !x_ref.stable then (
              Option.may (add_infl x) orig;
              x_ref := {!x_ref with wpoint = false; called = false};
            ) else (
              x_ref := {!x_ref with stable = true};
              (iterate[@tailcall]) orig x
            )
          ) else (
            (* old != wpd*)
            if tracing then trace "update" "%d set %a value: %a" job_id S.Var.pretty_trace x S.Dom.pretty wpd;
            let w = !x_ref.infl in
            x_ref := {!x_ref with rho = wpd; infl = VS.empty};
            destabilize w;
            if !x_ref.stable then (
              Option.may (add_infl x) orig;
              x_ref := {!x_ref with called = false};
            ) else (
              x_ref := {!x_ref with stable = true};
              (iterate[@tailcall]) orig x
            )
          )
        and add_side_to_struct y d = 
          (* TODO: rename to match new terminology *)
          let revive_suspended () =
            (* Preliminary results must be revaluated *)
            GobMutex.lock prom_mutex;
            List.iter (fun (is_primary, z, rsd, id) ->
                if tracing then trace "revive" "reviving job %d solving for %a" id S.Var.pretty_trace z;
                (* Question: Is it on purpose that we do not give a new ID here? *)
                promises := (Thread_pool.add_work pool (fun () -> solve_single is_primary z rsd id))::!promises
              ) !prelim_vars;
            prelim_vars := [];
            GobMutex.unlock prom_mutex
          in
          (* TODO: Does doing it in this order somehow effect the locality of sides? *)
          Sides.add_side job_id revive_suspended (y, d)
        and handle_side (y, v) =
          let y_ref = init unknowns y in
          let old_v = !y_ref.rho in
          if S.Dom.leq v old_v then 
            ()
          else (
            let new_v = S.Dom.widen old_v (S.Dom.join old_v v) in
            if tracing then trace "update" "%d side set %a value: %a" job_id S.Var.pretty_trace y S.Dom.pretty new_v;
            y_ref := {!y_ref with rho = new_v};
            (* TODO: should this happen via side or here and traced differently *)
            add_side_to_struct y new_v;
            let w = !y_ref.infl in
            y_ref := {!y_ref with stable = true; infl = VS.empty};
            destabilize w
          )
        in

        (* begining of solve_single *)
        (* let x_poi_ref = try HM.find unknowns x_poi *)
        (* with Not_found -> failwith "solve_single: x_poi not found in unknowns" *)
        (* in *)


        let x_poi_ref = init unknowns x_poi in
        if (not (!x_poi_ref.stable)) then (
          x_poi_ref := {!x_poi_ref with stable = true; called = true};
          iterate None x_poi
        );

        (* Question: This does not actively wait, but checks for updates and suspends *)
        (* if needed. Suspend is called by process updates if necessary *)
        (* Suspend does not suspend the thread, but saves the variable, so that the thread can be revived *)
        (* Revival is actually a creation of a new thread! The old thread is stopped! *)
        (* However, it takes over the thread local data! *)
        let rec wait () =
          let suspend () =
            GobMutex.lock prom_mutex;
            if tracing then trace "suspend" "suspending job %d solving for %a (suspended_vars: %d)" job_id S.Var.pretty_trace x_poi (List.length !prelim_vars);
            prelim_vars := (is_primary, x_poi, sd, job_id)::!prelim_vars;
            GobMutex.unlock prom_mutex
          in
          match Sides.updates_or_fin job_id !obs suspend with
          | Sides.NewSide -> (
              if tracing then trace "wait" "%d processing new sides" job_id;
              obs := Sides.process_updates job_id !obs handle_side;
              if !x_poi_ref.stable then
                wait ()
              else (
                x_poi_ref := {!x_poi_ref with stable = true; called = true};
                iterate None x_poi;
                wait ())
            ) 
          | Sides.Fin -> if tracing then trace "wait" "%d all sides processed -> suspended" job_id
        in
        wait ()
      in

      (* beginning of main solve (initial mapping set above) *)
      (* let start_data = {(create_empty_data ()) with unknowns = HM.copy start_unknowns} in *)
      let start_data = create_start_data st in
      List.iter (fun v -> ignore @@ init start_data.unknowns v) vs;

      (* If we have multiple start variables vs, we might solve v1, then while solving v2 we side some global which v1 depends on with a new value. Then v1 is no longer stable and we have to solve it again. *)
      let phase_number = ref 0 in
      let rec solver () = (* as while loop in paper *)
        incr phase_number;
        let is_stable x = !(HM.find start_data.unknowns x).stable in
        let unstable_vs = List.filter (neg is_stable) vs in
        if unstable_vs <> [] then (
          if Logs.Level.should_log Debug then (
            if !phase_number = 1 then Logs.newline ();
            Logs.debug "Unstable solver start vars in %d. phase:" !phase_number;
            List.iter (fun v -> Logs.debug "\t%a" S.Var.pretty_trace v) unstable_vs;
            Logs.newline ();
            flush_all ();
          );
          List.iter (fun x ->
              if tracing then trace "multivar" "solving for %a" S.Var.pretty_trace x;
              Domainslib.Task.run pool (fun () -> 
                  let first_id = Atomic.fetch_and_add job_id_counter 1 in
                  solve_single true x start_data first_id; 
                  (* make sure, everything is awaited, since promises could change during await_all *)
                  let rec await_changing_list () = 
                    GobMutex.lock prom_mutex;
                    let current_proms = !promises in
                    promises := [];
                    GobMutex.unlock prom_mutex;
                    Thread_pool.await_all pool current_proms;
                    let promises_empty = begin
                      GobMutex.lock prom_mutex;
                      let is_empty = List.is_empty !promises in
                      GobMutex.unlock prom_mutex;
                      is_empty 
                    end
                    in
                    if not promises_empty then await_changing_list ()
                  in
                  await_changing_list ();
                  if tracing then trace "dbg_para" "promises: %d" (List.length !promises)
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
      (*print_data_verbose data "Data after iterate completed";

        if GobConfig.get_bool "dbg.print_wpoints" then (
        Logs.newline ();
        Logs.debug "Widening points:";
        HM.iter (fun k () -> Logs.debug "%a" S.Var.pretty_trace k) wpoint;
        Logs.newline ();
        );*)

      (* TODO: make a better merge here*)
      if tracing then trace "dbg_para" "suspended_vars: %d" (List.length !prelim_vars);
      let unknowns_to_rho u = HM.map (fun k v -> !v.rho) u in
      let start_rho = unknowns_to_rho start_data.unknowns in
      let final_rho = List.fold (fun acc (_,_,sd,job_id) -> 
          if tracing then trace "dbg_para" "merging rho from job %d" job_id;
          HM.merge (
            fun k ao bo -> 
              match ao, bo with
              | None, None -> None
              | Some a, None -> ao
              | None, Some b -> bo
              | Some a, Some b -> if S.Dom.equal a b then ao 
                else (if tracing then trace "dbg_para" "Inconsistent data for %a:\n left: %a\n right (%d): %a" S.Var.pretty_trace k S.Dom.pretty a job_id S.Dom.pretty b; Some (S.Dom.join a b)) 
          ) acc (unknowns_to_rho sd.unknowns)
        ) start_rho !prelim_vars in
      if tracing then trace "dbg_para" "final_rho len: %d" (HM.length final_rho);
      final_rho
  end

let () =
  Selector.add_creating_eq_solver ("td_parallel_dist_ref", (module PostSolver.CreatingEqIncrSolverFromCreatingEqSolver (Base)));
