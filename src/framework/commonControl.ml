(** Shared analysis infrastructure for control.ml and fwdControl.ml. *)

open Batteries
open GoblintCil
open MyCFG
open Analyses
open GobConfig
open Constraints
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

(** Initialization helpers shared between [control.ml] and [fwdControl.ml].
    Parameterized by [CSS] to access [G.spec], [GVar.spec], and [G.create_spec],
    which are present on both concrete equation system types but absent from
    [BaseGlobConstrSys]. *)
module CommonInits (CSS: CommonSpecSys) = struct
  open CSS

  let make_global_fast_xml f g =
    let open Printf in
    let print_globals k v =
      fprintf f "\n<glob><key>%s</key>%a</glob>" (XmlUtil.escape (EQSys.GVar.var_id k)) EQSys.G.printXml v;
    in
    GHT.iter print_globals g

  let print_globals glob =
    let out = M.get_out (Spec.name ()) !M.out in
    let print_one v st =
      ignore (Pretty.fprintf out "%a -> %a\n" EQSys.GVar.pretty_trace v EQSys.G.pretty st)
    in
    GHT.iter print_one glob

  (* add extern variables to local state *)
  let do_extern_inits man (file : file) : Spec.D.t =
    let module VS = Set.Make (Basetype.Variables) in
    let add_glob s = function
        GVar (v,_,_) -> VS.add v s
      | _            -> s
    in
    let vars = foldGlobals file add_glob VS.empty in
    let set_bad v st =
      Spec.assign {man with local = st} (var v) MyCFG.unknown_exp
    in
    let is_std = function
      | {vname = ("__tzname" | "__daylight" | "__timezone"); _} (* unix time.h *)
      | {vname = ("tzname" | "daylight" | "timezone"); _} (* unix time.h *)
      | {vname = "getdate_err"; _} (* unix time.h, but somehow always in MacOS even without include *)
      | {vname = ("stdin" | "stdout" | "stderr"); _} (* standard stdio.h *)
      | {vname = ("optarg" | "optind" | "opterr" | "optopt" ); _} (* unix unistd.h *)
      | {vname = ("__environ"); _} -> (* Linux Standard Base Core Specification *)
        true
      | _ -> false
    in
    let add_externs s = function
      | GVarDecl (v,_) when not (VS.mem v vars || isFunctionType v.vtype) && not (get_bool "exp.hide-std-globals" && is_std v) -> set_bad v s
      | _ -> s
    in
    foldGlobals file add_externs (Spec.startstate MyCFG.dummy_func.svar)

  (* Simulate globals before analysis. *)
  (* TODO: make extern/global inits part of constraint system so all of this would be unnecessary. *)
  let do_global_inits ~getg ~sideg (file: file) : Spec.D.t * fundec list =
    let man =
      { ask     = (fun (type a) (q: a Queries.t) -> Queries.Result.top q)
      ; emit   = (fun _ -> failwith "Cannot \"emit\" in global initializer context.")
      ; node    = MyCFG.dummy_node
      ; prev_node = MyCFG.dummy_node
      ; control_context = (fun () -> man_failwith "Global initializers have no context.")
      ; context = (fun () -> man_failwith "Global initializers have no context.")
      ; edge    = MyCFG.Skip
      ; local   = Spec.D.top ()
      ; global  = (fun g -> g_spec (getg (gvar_spec g)))
      ; spawn   = (fun ?(multiple=false) _ -> failwith "Global initializers should never spawn threads. What is going on?")
      ; split   = (fun _ -> failwith "Global initializers trying to split paths.")
      ; sideg   = (fun g d -> sideg (gvar_spec g) (g_create_spec d))
      }
    in
    let edges = CfgTools.getGlobalInits file in
    Logs.debug "Executing %d assigns." (List.length edges);
    let funs = ref [] in
    (*let count = ref 0 in*)
    let transfer_func (st : Spec.D.t) (loc, edge) : Spec.D.t =
      if M.tracing then M.trace "con" "Initializer %a" CilType.Location.pretty loc;
      (*incr count;
        if (get_bool "dbg.verbose")&& (!count mod 1000 = 0)  then Printf.printf "%d %!" !count;    *)
      match edge with
      | MyCFG.Entry func        ->
        if M.tracing then M.trace "global_inits" "Entry %a" d_lval (var func.svar);
        Spec.body {man with local = st} func
      | MyCFG.Assign (lval,exp) ->
        if M.tracing then M.trace "global_inits" "Assign %a = %a" d_lval lval d_exp exp;
        begin match lval, exp with
          | (Var v,o), (AddrOf (Var f,NoOffset))
            when v.vstorage <> Static && isFunctionType f.vtype ->
            (try funs := Cilfacade.find_varinfo_fundec f :: !funs with Not_found -> ())
          | _ -> ()
        end;
        let res = Spec.assign {man with local = st} lval exp in
        (* Needed for privatizations (e.g. None) that do not side immediately *)
        let res' = Spec.sync {man with local = res} `Normal in
        if M.tracing then M.trace "global_inits" "\t\t -> state:%a" Spec.D.pretty res;
        res'
      | _                       -> failwith "Unsupported global initializer edge"
    in
    let transfer_func st (loc, edge) =
      let old_loc = !Goblint_tracing.current_loc in
      Goblint_tracing.current_loc := loc;
      (* TODO: next_loc? *)
      Goblint_backtrace.protect ~mark:(fun () -> TfLocation loc) ~finally:(fun () ->
          Goblint_tracing.current_loc := old_loc;
        ) (fun () ->
          transfer_func st (loc, edge)
        )
    in
    let with_externs = do_extern_inits man file in
    (*if (get_bool "dbg.verbose") then Printf.printf "Number of init. edges : %d\nWorking:" (List.length edges);    *)
    let result : Spec.D.t = List.fold_left transfer_func with_externs edges in
    if M.tracing then M.trace "global_inits" "startstate: %a" Spec.D.pretty result;
    result, !funs

  let enter_with ~getg ~sideg st fd =
    let st = st fd.svar in
    let man =
      { ask     = (fun (type a) (q: a Queries.t) -> Queries.Result.top q)
      ; emit   = (fun _ -> failwith "Cannot \"emit\" in enter_with context.")
      ; node    = MyCFG.dummy_node
      ; prev_node = MyCFG.dummy_node
      ; control_context = (fun () -> man_failwith "enter_with has no control_context.")
      ; context = Spec.startcontext
      ; edge    = MyCFG.Skip
      ; local   = st
      ; global  = (fun g -> g_spec (getg (gvar_spec g)))
      ; spawn   = (fun ?(multiple=false) _ -> failwith "Bug1: Using enter_func for toplevel functions with 'otherstate'.")
      ; split   = (fun _ -> failwith "Bug2: Using enter_func for toplevel functions with 'otherstate'.")
      ; sideg   = (fun g d -> sideg (gvar_spec g) (g_create_spec d))
      }
    in
    let args = List.map (fun _ -> MyCFG.unknown_exp) fd.sformals in
    let ents = Spec.enter man None fd args in
    List.map (fun (_,s) -> fd, s) ents

  let otherstate ~getg ~sideg st v =
    let man =
      { ask     = (fun (type a) (q: a Queries.t) -> Queries.Result.top q)
      ; emit   = (fun _ -> failwith "Cannot \"emit\" in otherstate context.")
      ; node    = MyCFG.dummy_node
      ; prev_node = MyCFG.dummy_node
      ; control_context = (fun () -> man_failwith "enter_func has no context.")
      ; context = (fun () -> man_failwith "enter_func has no context.")
      ; edge    = MyCFG.Skip
      ; local   = st
      ; global  = (fun g -> g_spec (getg (gvar_spec g)))
      ; spawn   = (fun ?(multiple=false) _ -> failwith "Bug1: Using enter_func for toplevel functions with 'otherstate'.")
      ; split   = (fun _ -> failwith "Bug2: Using enter_func for toplevel functions with 'otherstate'.")
      ; sideg   = (fun g d -> sideg (gvar_spec g) (g_create_spec d))
      }
    in
    (* TODO: don't hd *)
    List.hd (Spec.threadenter man ~multiple:false None v [])
  (* TODO: do threadspawn to mainfuns? *)

  let make_man ~getg ~sideg e =
    { ask     = (fun (type a) (q: a Queries.t) -> Queries.Result.top q)
    ; emit   = (fun _ -> failwith "Cannot \"emit\" in enter_with context.")
    ; node    = MyCFG.dummy_node
    ; prev_node = MyCFG.dummy_node
    ; control_context = (fun () -> man_failwith "enter_with has no control_context.")
    ; context = Spec.startcontext
    ; edge    = MyCFG.Skip
    ; local   = e
    ; global  = (fun g -> g_spec (getg (gvar_spec g)))
    ; spawn   = (fun ?(multiple=false) _ -> failwith "Bug1: Using enter_func for toplevel functions with 'otherstate'.")
    ; split   = (fun _ -> failwith "Bug2: Using enter_func for toplevel functions with 'otherstate'.")
    ; sideg   = (fun g d -> sideg (gvar_spec g) (g_create_spec d))
    }

  let compute_startvars ~getg ~sideg startstate startfuns exitfuns otherfuns =
    (try MyCFG.dummy_func.svar.vdecl <- (List.hd otherfuns).svar.vdecl with Failure _ -> ());
    let startvars =
      if startfuns = []
      then [[MyCFG.dummy_func, startstate]]
      else
        let morph f = Spec.morphstate f startstate in
        List.map (enter_with ~getg ~sideg morph) startfuns
    in
    let exitvars = List.map (enter_with ~getg ~sideg Spec.exitstate) exitfuns in
    let prestartstate = Spec.startstate MyCFG.dummy_func.svar in (* like in do_extern_inits *)
    let othervars = List.map (enter_with ~getg ~sideg (otherstate ~getg ~sideg prestartstate)) otherfuns in
    let all = List.concat (startvars @ exitvars @ othervars) in
    if all = [] then
      failwith "BUG: Empty set of start variables; may happen if enter_func of any analysis returns an empty list.";
    all

end
