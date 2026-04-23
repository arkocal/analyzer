(** Shared analysis infrastructure for control.ml and fwdControl.ml. *)

open GoblintCil
open MyCFG
open Analyses
open GobConfig
open SpecLifters

module type S2S = Spec2Spec

(* spec is lazy, so HConsed table in Hashcons lifters is preserved between analyses in server mode *)
(** Builds a [Spec'] module. [~fwd:true] adds [NoDigestLifter] and adjusts the
    [PathSensitive2] condition for the forward solver. *)
let make_spec_module ~fwd : (module Spec') Lazy.t = lazy (
  GobConfig.building_spec := true;
  let arg_enabled = get_bool "exp.arg.enabled" in
  let termination_enabled = List.mem "termination" (get_string_list "ana.activated") in
  let lift opt (module F : S2S) (module X : Spec) = (module (val if opt then (module F (X)) else (module X) : Spec) : Spec) in
  let module S1 =
    (val
      (module MCP.MCP2 : Spec)
      |> lift (get_int "ana.context.gas_value" >= 0) (ContextGasLifter.get_gas_lifter ())
      |> lift true (module WidenContextLifterSide) (* option checked in functor *)
      |> lift (get_int "ana.widen.delay.local" > 0) (module WideningDelay.DLifter)
      (* hashcons before witness to reduce duplicates, because witness re-uses contexts in domain and requires tag for PathSensitive3 *)
      |> lift (get_bool "ana.opt.hashcons" || arg_enabled) (module HashconsContextLifter)
      |> lift (get_bool "ana.opt.hashcached") (module HashCachedContextLifter)
      |> lift arg_enabled (module HashconsLifter)
      |> lift arg_enabled (module ArgConstraints.PathSensitive3)
      |> lift (not arg_enabled && (not fwd || not (get_bool "solvers.fwd.digests"))) (module PathSensitive2)
      |> lift (get_bool "ana.dead-code.branches") (module DeadBranchLifter)
      |> lift true (module DeadCodeLifter)
      |> lift (get_bool "dbg.slice.on") (module LevelSliceLifter)
      |> lift (get_bool "ana.opt.equal" && not (get_bool "ana.opt.hashcons")) (module OptEqual)
      |> lift (get_bool "ana.opt.hashcons") (module HashconsLifter)
      (* Widening tokens must be outside of hashcons, because widening token domain ignores token sets for identity, so hashcons doesn't allow adding tokens.
         Also must be outside of deadcode, because deadcode splits (like mutex lock event) don't pass on tokens. *)
      |> lift (get_bool "ana.widen.tokens") (module WideningTokenLifter.Lifter)
      |> lift true (module LongjmpLifter.Lifter)
      |> lift termination_enabled (module RecursionTermLifter.Lifter) (* Always activate the recursion termination analysis, when the loop termination analysis is activated*)
      |> lift (get_int "ana.widen.delay.global" > 0) (module WideningDelay.GLifter)
      |> lift (fwd && not (get_bool "solvers.fwd.digests")) (module NoDigestLifter.Lifter)
    )
  in
  GobConfig.building_spec := false;
  ControlSpecC.control_spec_c := (module S1.C);
  let module S1 = Spec2Spec' (S1) in
  (module S1)
)

(* This function was originally a part of the [AnalyzeCFG] module, but
   now that [AnalyzeCFG] takes [Spec] as a functor parameter,
   [analyze_loop] cannot reside in it anymore since each invocation of
   [get_spec] in the loop might/should return a different module, and we
   cannot swap the functor parameter from inside [AnalyzeCFG]. *)
let make_analyze_loop get_spec analyze_body =
  let rec loop (module CFG : CfgBidirSkip) file fs change_info =
    try
      let (module Spec : Spec') = get_spec () in
      analyze_body (module CFG : CfgBidirSkip) (module Spec : Spec') file fs change_info
    with Refinement.RestartAnalysis ->
      loop (module CFG) file fs change_info
  in
  loop

let analyze analyze_loop change_info file fs =
  Logs.debug "Generating the control flow graph.";
  let (module CFG : CfgBidirSkip) = CfgTools.compute_cfg file in
  MyCFG.current_cfg := (module CFG);
  analyze_loop (module CFG : CfgBidirSkip) file fs change_info
