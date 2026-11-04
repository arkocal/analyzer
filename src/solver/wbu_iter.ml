(** Non-recursive version of {!Wbu}.

    Follows the same evaluation order as {!Wbu}, but keeps what would be on the
    call stack in per-unknown frames. An unknown is on the DFS path at most once
    (its [called] flag blocks re-entry), so one frame per unknown is enough. The
    [finisher] fields link the frames on the path into a stack, and the solver
    runs in a loop that holds at most one active frame. *)

open Goblint_constraint.ConstrSys

module FwdWBuIterSolver (System: FwdGlobConstrSys) = struct

  module Base = FwdCommon.BaseFwdSolver(System)
  open Base

  open FwdCommon.SolverStats(System)

  module WorkSet = (val
                     match GobConfig.get_string "solvers.fwd.work_iteration" with
                     | "lifo" -> (module FwdCommon.LIFOWorkList(System.LVar) : FwdCommon.WorkListS with type elt = System.LVar.t)
                     | "set"  -> (module FwdCommon.SetWorkList(System.LVar)  : FwdCommon.WorkListS with type elt = System.LVar.t)
                     | s      -> failwith ("Unknown work_iteration strategy: " ^ s)
                   )

  let abort = GobConfig.get_bool "solvers.bu.abort"

  let always_warrow = GobConfig.get_bool "solvers.bu.always_warrow"

  (** Solver state added on top of {!Lcl}, which {!FwdCommon.Checker} requires
      to stay as it is. *)
  module Frames = struct
    (** A contribution that an unknown still has to make. Both kinds are needed
        because with local GC, global updates run after contributions that may
        descend. *)
    type todo =
      | Local of System.LVar.t * D.t
      | Global of System.GVar.t * G.t

    type t = {
      x: System.LVar.t;
      (** Number of the frame, which is cheaper to compare than [x]. *)
      id: int;
      record: Lcl.t;
      mutable todos: todo list;
      (** The frame to return to when this one is done. [None] for a root
          picked from the worklist. *)
      mutable finisher: t option;
    }

    let table: t LM.t = LM.create 100

    let next_id = ref 0

    (** Use this instead of [Lcl.get], so that the frame and the record come
        from one lookup. *)
    let get x =
      match LM.find_opt table x with
      | Some frame -> frame
      | None ->
        let id = !next_id in
        incr next_id;
        let frame = {x; id; record = Lcl.get x; todos = []; finisher = None} in
        LM.add table x frame;
        frame
  end

  open Frames

  (** Globals that avoid the lookups and key comparisons of {!Base.Gbl}. *)
  module Gbl = struct
    module OM = Base.Gbl.OM
    module GWarrow = Base.Gbl.GWarrow

    (** Ordered like [LS], so that joining the values visits unknowns in the
        same order as folding over a set of them. *)
    module LMap = Map.Make (System.LVar)

    module IMap = Map.Make (Stdlib.Int)

    type contribution = {
      warrow: GWarrow.contribution;
      (** Newest value from each unknown at this origin. *)
      values: G.t LMap.t;
      (** Join of [values], before warrowing. *)
      joined: G.t;
    }

    type t = {
      value: G.t;
      init: G.t;
      (** Frames by id, so that adding one does not have to compare unknowns.
          They are visited in id order, which does not matter for the [set]
          worklist, but means that [lifo] can differ from {!Wbu}. *)
      infl: Frames.t IMap.t;
      from: contribution OM.t;
    }

    let glob: t GM.t = GM.create 100

    let get g =
      match GM.find_opt glob g with
      | Some record -> record
      | None ->
        let record = {value = G.bot (); init = G.bot (); infl = IMap.empty; from = OM.create 10} in
        GM.add glob g record;
        record

    let init (g, d) =
      GM.add glob g {value = d; init = d; infl = IMap.empty; from = OM.create 10}

    (** Adding a binding that is already there returns the same map. *)
    let add_infl record g frame =
      let infl = IMap.add frame.id frame record.infl in
      if infl != record.infl then GM.replace glob g {record with infl}

    let replace = GM.replace glob

    let construct_value record =
      OM.fold (fun _ c a -> G.join a c.warrow.value) record.from record.init

    let get_contribution origin record =
      match OM.find_opt record.from origin with
      | Some contribution -> contribution
      | None ->
        let contribution = {warrow = GWarrow.default (); values = LMap.empty; joined = G.bot ()} in
        OM.add record.from origin contribution;
        contribution

    (** Returns the record with its new value if the value changed. The record
        is not stored, since the caller changes it again and stores it once. *)
    let update_contribution x g d =
      let origin = System.LVar.node x in
      let record = get g in
      let old_contribution = get_contribution origin record in
      let values, joined =
        match LMap.find_opt x old_contribution.values with
        | Some old_d when G.equal old_d d ->
          (* Same value from this unknown, so the join stays the same as well. *)
          old_contribution.values, old_contribution.joined
        | _ ->
          let values = LMap.add x d old_contribution.values in
          values, LMap.fold (fun _ v a -> G.join a v) values (G.bot ()) in
      let warrow = GWarrow.warrow old_contribution.warrow joined in
      OM.replace record.from origin {warrow; values; joined};

      if G.equal warrow.value old_contribution.warrow.value then None
      else (
        let new_g = if G.leq old_contribution.warrow.value warrow.value then
            G.join warrow.value record.value
          else construct_value record in
        if G.equal record.value new_g then None
        else Some {record with value = new_g}
      )
  end

  (** Read-only view of {!Gbl} in the shape that {!FwdCommon.Checker} requires.
      It only reads [value] and [infl]. Only for use after solving: a view is
      built once per global, since the checker asks for it on every side effect. *)
  module CheckerGbl = struct
    module Sys = System
    module G = G
    module LM = LM
    module LS = LS
    module GM = GM
    module OM = Gbl.OM

    type contribution = Gbl.contribution

    type t = {value: G.t; init: G.t; infl: LS.t; last: G.t LM.t; from: contribution OM.t}

    let unused_last: G.t LM.t = LM.create 1

    let views: t GM.t = GM.create 100

    let get g =
      match GM.find_opt views g with
      | Some view -> view
      | None ->
        let record = Gbl.get g in
        let view = {
          value = record.value;
          init = record.init;
          infl = Gbl.IMap.fold (fun _ frame s -> LS.add frame.x s) record.infl LS.empty;
          last = unused_last;
          from = record.from;
        } in
        GM.add views g view;
        view

    let get_contribution origin record = OM.find record.from origin
  end

  (** Also drops the pending local contributions of the frame, which would be
      skipped anyway since it will be re-evaluated. Global contributions are not
      affected by aborting. *)
  let mark_aborted frame =
    if abort && not frame.record.aborted then
      frame.todos <- List.filter (function Global _ -> true | Local _ -> false) frame.todos;
    frame.record.aborted <- true

  let get_global frame g =
    let glob_data = Gbl.get g in
    Gbl.add_infl glob_data g frame;
    glob_data.value

  let get_local _ = raise (Failure "Locals should not be queried in rhs")

  let set_global x g d =
    match Gbl.update_contribution x g d with
    | Some g_record ->
      let work = g_record.infl in
      Gbl.replace g {g_record with infl = Gbl.IMap.empty};
      let reevaluate _ frame =
        if frame.record.called then mark_aborted frame
        else WorkSet.add frame.x in
      Gbl.IMap.iter reevaluate work
    | None -> ()

  (** Evaluate the rhs of a frame, and set its todos to the resulting
      contributions, in the order of {!FwdCommon.BaseFwdSolver.wrap}. *)
  let evaluate frame rhs =
    let x = frame.x in
    let x_record = frame.record in
    eval_rhs_event x;
    x_record.called <- true;
    x_record.aborted <- false;

    let local_updates = LM.create 10 in
    let global_updates = GM.create 10 in

    let collect_local y d =
      let d = LM.find_opt local_updates y |> BatOption.map_default (D.join d) d in
      LM.replace local_updates y d in

    let collect_global g d =
      let d = GM.find_opt global_updates g |> BatOption.map_default (G.join d) d in
      GM.replace global_updates g d in

    let old_global_contribs = x_record.global_contribs in
    let old_local_contribs = x_record.local_contribs in
    rhs x_record.loc_value get_local collect_local (get_global frame) collect_global;

    (* Built in reverse. Folds visit elements in the same order as iter. *)
    let todos = [] in
    let todos =
      if do_global_gc then (
        let new_contribs = GM.fold (fun g _ s -> GS.add g s) global_updates GS.empty in
        x_record.global_contribs <- new_contribs;
        let removed_contribs = GS.filter (fun g -> not @@ GS.mem g new_contribs) old_global_contribs in
        GS.fold (fun g acc -> Global (g, G.bot ()) :: acc) removed_contribs todos
      ) else todos in
    let todos =
      if do_local_gc then (
        let new_contribs = LM.fold (fun l _ s -> LS.add l s) local_updates LS.empty in
        x_record.local_contribs <- new_contribs;
        let removed_contribs = LS.filter (fun l -> not @@ LS.mem l new_contribs) old_local_contribs in
        LS.fold (fun l acc -> Local (l, D.bot ()) :: acc) removed_contribs todos
      ) else todos in
    let todos = GM.fold (fun g d acc -> Global (g, d) :: acc) global_updates todos in
    let todos = LM.fold (fun y d acc -> Local (y, d) :: acc) local_updates todos in
    frame.todos <- List.rev todos

  (** Start iterating a frame, and return the frame that is active afterwards. *)
  let activate frame finisher =
    match System.system frame.x with
    | None -> finisher
    | Some rhs ->
      frame.finisher <- finisher;
      evaluate frame rhs;
      Some frame

  (** {!Lcl.update_contribution} for a record that is already at hand. Returns
      whether the value of the record changed. *)
  let update_local contributor (y_record: Lcl.t) d =
    let old_contribution = Lcl.get_contribution contributor y_record in
    let new_contribution =
      if always_warrow || y_record.called then Lcl.warrow old_contribution d
      else {old_contribution with Lcl.LWarrow.value = d} in
    let old_value = old_contribution.Lcl.LWarrow.value in
    let new_value = new_contribution.Lcl.LWarrow.value in
    if D.equal new_value old_value then false
    else (
      LM.replace y_record.loc_from contributor new_contribution;
      let new_y = if D.leq old_value new_value then
          D.join y_record.loc_value new_value
        else Lcl.construct_value y_record in
      if D.equal y_record.loc_value new_y then false
      else (
        y_record.loc_value <- new_y;
        true
      )
    )

  (** Make a local contribution from a frame, and return the frame that is
      active afterwards. *)
  let set_local frame y d =
    let current = Some frame in
    let y_frame = Frames.get y in
    if update_local frame.x y_frame.record d then (
      if y_frame.record.called then (
        mark_aborted y_frame;
        current
      )
      else (
        WorkSet.remove y;
        activate y_frame current
      )
    )
    else current

  let rec run = function
    | None -> ()
    | Some frame as current ->
      match frame.todos with
      | Global (g, d) :: todos ->
        frame.todos <- todos;
        set_global frame.x g d;
        (run[@tailcall]) current
      | Local (y, d) :: todos ->
        frame.todos <- todos;
        (run[@tailcall]) (set_local frame y d)
      | [] ->
        frame.record.called <- false;
        if frame.record.aborted then
          (run[@tailcall]) (activate frame frame.finisher)
        else (
          let finisher = frame.finisher in
          frame.finisher <- None;
          (run[@tailcall]) finisher
        )

  let iterate x = run (activate (Frames.get x) None)

  module Checker = FwdCommon.Checker(System)(Lcl)(CheckerGbl)

  let solve localinit globalinit start_unknowns =
    solver_start_event ();
    List.iter Lcl.init localinit;
    List.iter Gbl.init globalinit;
    List.iter WorkSet.add start_unknowns;
    WorkSet.map_until_empty iterate;
    solver_end_event ();
    AnalysisState.should_warn := true;
    AnalysisState.postsolving := true;
    Checker.check localinit globalinit start_unknowns
end
