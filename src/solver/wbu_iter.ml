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

  (** {!Base.Gbl}, except that [update_contribution] uses [LM.replace] on the
      [last] table. Only the newest binding of a key is ever read, and with
      [LM.add] the older ones would stay reachable and lengthen bucket chains. *)
  module Gbl = struct
    include Base.Gbl

    let update_contribution x g d =
      let sx = source x in
      let g_record = get g in
      let old_contribution = get_contribution sx g_record in
      LM.replace g_record.last x d;
      let set = LS.add x old_contribution.set in
      let d_new = get_last_contrib set g_record.last in
      let new_contribution = warrow old_contribution d_new set in
      OM.replace g_record.from sx new_contribution;

      if G.equal new_contribution.value old_contribution.value then (NotUpdated g_record)
      else (
        let new_g = if G.leq old_contribution.value new_contribution.value then
            G.join new_contribution.value g_record.value
          else construct_value g_record in
        if (G.equal g_record.value new_g) then (NotUpdated g_record)
        else (
          replace g {g_record with value=new_g};
          Updated {g_record with value=new_g}
        )
      )
  end
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
      record: Lcl.t;
      mutable todos: todo list;
      (** The frame to return to when this one is done. [None] for a root
          picked from the worklist. *)
      mutable finisher: t option;
    }

    let table: t LM.t = LM.create 100

    let get x record =
      match LM.find_opt table x with
      | Some frame -> frame
      | None ->
        let frame = {x; record; todos = []; finisher = None} in
        LM.add table x frame;
        frame

    (** Drop the pending local contributions of an unknown on the DFS path. They
        would be skipped anyway, since it will be re-evaluated. Global
        contributions are not affected by aborting. *)
    let drop_locals x =
      let frame = LM.find table x in
      frame.todos <- List.filter (function Global _ -> true | Local _ -> false) frame.todos
  end

  open Frames

  let mark_aborted x (record: Lcl.t) =
    if abort && not record.aborted then drop_locals x;
    record.aborted <- true

  let get_global x g =
    let glob_data = Gbl.get g in
    Gbl.add_infl glob_data g x;
    glob_data.value

  let get_local _ = raise (Failure "Locals should not be queried in rhs")

  let set_global x g d =
    match Gbl.update_contribution x g d with
    | Updated g_record -> (
        let work = g_record.infl in
        Gbl.replace g {g_record with infl = LS.empty};
        let reevaluate x =
          let r = Lcl.get x in
          if r.called then mark_aborted x r
          else WorkSet.add x in
        LS.iter reevaluate work
      )
    | NotUpdated _ -> ()

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
    rhs x_record.loc_value get_local collect_local (get_global x) collect_global;

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

  (** Start iterating [x], and return the frame that is active afterwards. *)
  let activate x finisher =
    let record = Lcl.get x in
    match System.system x with
    | None -> finisher
    | Some rhs ->
      let frame = Frames.get x record in
      frame.finisher <- finisher;
      evaluate frame rhs;
      Some frame

  (** Make a local contribution from a frame, and return the frame that is
      active afterwards. *)
  let set_local frame y d =
    let current = Some frame in
    match Lcl.update_contribution frame.x y d always_warrow with
    | Updated y_record ->
      if y_record.called then (
        mark_aborted y y_record;
        current
      )
      else (
        WorkSet.remove y;
        activate y current
      )
    | NotUpdated _ -> current

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
          (run[@tailcall]) (activate frame.x frame.finisher)
        else (
          let finisher = frame.finisher in
          frame.finisher <- None;
          (run[@tailcall]) finisher
        )

  let iterate x = run (activate x None)

  module Checker = FwdCommon.Checker(System)(Lcl)(Gbl)

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
