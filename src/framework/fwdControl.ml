(** Main internal functionality: analysis of the program by abstract interpretation via constraint solving. *)

(** An analyzer that takes the CFG from [MyCFG], a solver from [Selector], constraints from [Constraints] (using the specification from [MCP]) *)

open Batteries
open GoblintCil
open MyCFG
open Analyses
open Goblint_constraint.ConstrSys
open Goblint_constraint.Translators
open Goblint_constraint.SolverTypes
open GobConfig
open Constraints
(* spec is lazy, so HConsed table in Hashcons lifters is preserved between analyses in server mode *)
let spec_module = CommonControl.make_spec_module ~fwd:true

(** gets Spec for current options *)
let get_spec (): (module Spec') =
  Lazy.force spec_module

let current_node_state_json : (Node.t -> Yojson.Safe.t option) ref = ref (fun _ -> None)

let current_varquery_global_state_json: (Goblint_constraint.VarQuery.t option -> Yojson.Safe.t) ref = ref (fun _ -> `Null)

(** Given a [Cfg], a [Spec], and an [Inc], computes the solution to [MCP.Path] *)
module AnalyzeCFG (Cfg:CfgBidirSkip) (Spec:Spec') (Inc:Increment) =
struct

  module SpecSys: FwdSpecSys with module Spec = Spec =
  struct
    (* Must be created in module, because cannot be wrapped in a module later. *)
    module Spec = Spec

    (* The Equation system *)
    module EQSys = FwdConstraints.FromSpec (Spec) (Cfg) (Inc)

    (* Hashtbl for locals *)
    module LHT   = BatHashtbl.Make (EQSys.LVar)
    (* Hashtbl for globals *)
    module GHT   = BatHashtbl.Make (EQSys.GVar)
  end

  open SpecSys

  module CSS = struct
    include SpecSys
    let g_spec = EQSys.G.spec
    let gvar_spec = EQSys.GVar.spec
    let g_create_spec = EQSys.G.create_spec
  end
  module I = CommonControl.CommonInits (CSS)

  (* The solver *)
  module PostSolverArg =
  struct
    let should_prune = true
    let should_verify = get_bool "verify"
    let should_warn = get_string "warn_at" <> "never"
    let should_save_run =
      (* copied from solve_and_postprocess *)
      let gobview = get_bool "gobview" in
      let save_run = let o = get_string "save_run" in if o = "" then (if gobview then "run" else "") else o in
      save_run <> ""
  end

  module Sys = FwdConstraints.FromSpec (Spec) (Cfg) (Inc)
  module FwdSolver = FwdSolver.FwdSolver (Sys)
  module BuSolver = Bu.FwdBuSolver (Sys)
  module WBuSolver = Wbu.FwdWBuSolver (Sys)
  (* module Slvr2 = BuSlvr *)

  module CompareGlobSys = FwdCompareConstraints.CompareGlobSys (SpecSys)
  (* TODO remove those witnesses once the mismatch is solved *)
  let _ : EQSys.G.t -> EQSys.G.t = fun x -> x
  let _ : 'a CompareGlobSys.GH.t -> 'a GHT.t = fun x -> x


  (* Triple of the function, context, and the local value. *)
  module RT = AnalysisResult.ResultType2Digest (Spec)
  (* Set of triples [RT] *)
  module LT = SetDomain.HeadlessSet (RT)
  (* Analysis result structure---a hashtable from program points to [LT] *)
  module Result = AnalysisResult.Result (LT) (struct let result_name = "analysis" end)
  module ResultOutput = AnalysisResultOutput.Make (Result)



  (* print out information about dead code *)
  let print_dead_code (xs:Result.t) uncalled_fn_loc =
    let module NH = Hashtbl.Make (Node) in
    let live_nodes : unit NH.t = NH.create 10 in
    let count = ref 0 in (* Is only populated if "ana.dead-code.lines" or "ana.dead-code.branches" is true *)
    let module StringMap = BatMap.Make (String) in
    let live_lines = ref StringMap.empty in
    let dead_lines = ref StringMap.empty in
    let module FunSet = Hashtbl.Make (CilType.Fundec) in
    let live_funs: unit FunSet.t = FunSet.create 13 in
    let add_one n v =
      match n with
      | Statement s when Cilfacade.(StmtH.mem pseudo_return_to_fun s) ->
        (* Exclude pseudo returns from dead lines counting. No user code at "}". *)
        ()
      | _ ->
        (* Not using Node.location here to have updated locations in incremental analysis.
           See: https://github.com/goblint/analyzer/issues/290#issuecomment-881258091. *)
        let l = UpdateCil.getLoc n in
        let f = Node.find_fundec n in
        FunSet.replace live_funs f ();
        let add_fun  = BatISet.add l.line in
        let add_file = StringMap.modify_def BatISet.empty f.svar.vname add_fun in
        let is_dead = LT.for_all (fun (_,_,_,x) -> Spec.D.is_bot x) v in
        if is_dead then (
          dead_lines := StringMap.modify_def StringMap.empty l.file add_file !dead_lines
        ) else (
          live_lines := StringMap.modify_def StringMap.empty l.file add_file !live_lines;
          NH.add live_nodes n ()
        );
    in
    Result.iter add_one xs;
    let live_count = StringMap.fold (fun _ file_lines acc ->
        StringMap.fold (fun _ fun_lines acc ->
            acc + ISet.cardinal fun_lines
          ) file_lines acc
      ) !live_lines 0
    in
    let live file fn =
      try StringMap.find fn (StringMap.find file !live_lines)
      with Not_found -> BatISet.empty
    in
    if List.mem "termination" @@ get_string_list "ana.activated" then (
      (* check if we have upjumping gotos *)
      let open Cilfacade in
      let warn_for_upjumps fundec gotos =
        if FunSet.mem live_funs fundec then (
          (* set nortermiantion flag *)
          AnalysisState.svcomp_may_not_terminate := true;
          (* iterate through locations to produce warnings *)
          LocSet.iter (fun l _ ->
              M.warn ~loc:(M.Location.CilLocation l) ~category:Termination "The program might not terminate! (Upjumping Goto)"
            ) gotos
        )
      in
      FunLocH.iter warn_for_upjumps funs_with_upjumping_gotos
    );
    dead_lines := StringMap.mapi (fun fi -> StringMap.mapi (fun fu ded -> BatISet.diff ded (live fi fu))) !dead_lines;
    dead_lines := StringMap.map (StringMap.filter (fun _ x -> not (BatISet.is_empty x))) !dead_lines;
    dead_lines := StringMap.filter (fun _ x -> not (StringMap.is_empty x)) !dead_lines;
    let warn_func file f xs =
      let warn_range b e =
        count := !count + (e - b + 1); (* for total count below *)
        let doc =
          if b = e then
            Pretty.dprintf "on line %d" b
          else
            Pretty.dprintf "on lines %d..%d" b e
        in
        let loc: Cil.location = {
          file;
          line = b;
          column = -1; (* not shown *)
          byte = 0; (* wrong, but not shown *)
          endLine = e;
          endColumn = -1; (* not shown *)
          endByte = 0; (* wrong, but not shown *)
          synthetic = false;
        }
        in
        (doc, Some (Messages.Location.CilLocation loc)) (* CilLocation is fine because always printed from scratch *)
      in
      let msgs =
        BatISet.fold_range (fun b e acc ->
            warn_range b e :: acc
          ) xs []
      in
      let msgs = List.rev msgs in (* lines in ascending order *)
      M.msg_group Warning ~category:Deadcode "Function '%s' has dead code" f msgs (* TODO: function location for group *)
    in
    let warn_file f = StringMap.iter (warn_func f) in
    if get_bool "ana.dead-code.lines" then (
      StringMap.iter warn_file !dead_lines; (* populates count by side-effect *)
      let severity: M.Severity.t = if StringMap.is_empty !dead_lines then Info else Warning in
      let dead_total = !count + uncalled_fn_loc in
      let total = live_count + dead_total in (* We can only give total LoC if we counted dead code *)
      M.msg_group severity ~category:Deadcode "Logical lines of code (LLoC) summary" [
        (Pretty.dprintf "live: %d" live_count, None);
        (Pretty.dprintf "dead: %d%s" dead_total (if uncalled_fn_loc > 0 then Printf.sprintf " (%d in uncalled functions)" uncalled_fn_loc else ""), None);
        (Pretty.dprintf "total lines: %d" total, None);
      ]
    );
    NH.mem live_nodes

  (* convert result that can be out-put *)
  let solver2source_result h : Result.t =
    (* processed result *)
    let res = Result.create 113 in

    (* Adding the state at each system variable to the final result *)
    (* TODO: Consider digest in result? *)
    let add_local_var (x : LHT.key) state =
      (* Not using Node.location here to have updated locations in incremental analysis.
          See: https://github.com/goblint/analyzer/issues/290#issuecomment-881258091. *)
      let loc = UpdateCil.getLoc x.node in
      if loc <> locUnknown then try
          if Result.mem res x.node then
            (* If this source location has been added before, we look it up
              * and add another node to it information to it. *)
            let prev = Result.find res x.node in
            Result.replace res x.node (LT.add (x.context,x.original_digest, x.current_digest,state) prev)
          else
            Result.add res x.node (LT.singleton (x.context,x.original_digest, x.current_digest,state))
        (* If the function is not defined, and yet has been included to the
          * analysis result, we generate a warning. *)
        with Not_found ->
          Messages.debug ~category:Analyzer ~loc:(CilLocation loc) "Calculated state for undefined function: unexpected node %a" Node.pretty_trace x.node
    in
    LHT.iter add_local_var h;
    res

  (** The main function to preform the selected analyses. *)
  let analyze (file: file) (startfuns, exitfuns, otherfuns: Analyses.fundecs) =
    let module FileCfg: FileCfg =
    struct
      let file = file
      module Cfg = Cfg
    end
    in

    AnalysisState.should_warn := false; (* reset for server mode *)

    (* Simulate globals before analysis. *)
    (* TODO: make extern/global inits part of constraint system so all of this would be unnecessary. *)
    let gh = GHT.create 13 in
    let getg v = GHT.find_default gh v (EQSys.G.bot ()) in
    let sideg v d =
      if M.tracing then M.trace "global_inits" "sideg %a = %a" EQSys.GVar.pretty v EQSys.G.pretty d;
      GHT.replace gh v (EQSys.G.join (getg v) d)
    in
    (* Old-style global function for context.
     * This indirectly prevents global initializers from depending on each others' global side effects, which would require proper solving. *)
    let getg_bot (_ : EQSys.GVar.t) : EQSys.G.t = EQSys.G.bot () in

    (* real beginning of the [analyze] function *)
    if get_bool "ana.sv-comp.enabled" then
      Witness.init (module FileCfg); (* TODO: move this out of analyze_loop *)
    YamlWitness.init ();

    AnalysisState.global_initialization := true;
    GobConfig.earlyglobs := get_bool "exp.earlyglobs";
    let marshal: Spec.marshal option = None in

    (* Some happen in init, so enable this temporarily (if required by option). *)
    AnalysisState.should_warn := PostSolverArg.should_warn;
    Spec.init marshal;
    Access.init file;
    AnalysisState.should_warn := false;

    let startstate, more_funs =
      Logs.debug "Initializing %d globals." (CfgTools.numGlobals file);
      Timing.wrap "global_inits" (I.do_global_inits ~getg:getg_bot ~sideg) file
    in

    let otherfuns = if get_bool "kernel" then otherfuns @ more_funs else otherfuns in

    let startfuns = I.compute_startvars ~getg ~sideg startstate startfuns exitfuns otherfuns in

    AnalysisState.global_initialization := false;

    let man e = I.make_man ~getg ~sideg e in
    let to_startvar (n,e) =
      let context = Spec.context (man e) n e in
      let digest = Spec.P.of_elt e in
      let startvar : LHT.key = {node = MyCFG.FunctionEntry n; context; original_digest = digest; current_digest = digest} in
      startvar
    in
    let startvars' =
      List.map to_startvar startfuns in

    let entrystates =
      let to_entrystate (n,e) =
        let startvar = to_startvar (n,e) in
        (startvar, e)
      in
      List.map to_entrystate startfuns in
    let entrystates_global = GHT.to_list gh in

    let uncalled_dead = ref 0 in

    let solve_and_postprocess () =
      (* handle save_run/load_run *)
      let solver_file = "solver.marshalled" in
      let load_run = get_string "load_run" in
      let compare_runs = get_string_list "compare_runs" in
      let gobview = get_bool "gobview" in
      let save_run_str = let o = get_string "save_run" in if o = "" then (if gobview then "run" else "") else o in
      let solve = if (get_string "solver" = "bu") then BuSolver.solve else
        if (get_string "solver" = "wbu") then WBuSolver.solve else FwdSolver.solve in
      let rho, tau = Timing.wrap "solving" (solve entrystates entrystates_global) startvars' in
      let lh, gh = LHT.of_seq rho, GHT.of_seq tau in

      (* Most warnings happen before during postsolver, but some happen later (e.g. in finalize), so enable this for the rest (if required by option). *)
      AnalysisState.should_warn := PostSolverArg.should_warn;
      let insrt (k : LHT.key) _ s = match k.node with
        | MyCFG.FunctionEntry fn -> Set.Int.add fn.svar.vid s
        | _ -> s
      in
      (* set of ids of called functions *)
      let calledFuns = LHT.fold insrt lh Set.Int.empty in
      let is_bad_uncalled fn loc =
        not (Set.Int.mem fn.vid calledFuns) &&
        not (Str.last_chars loc.file 2 = ".h") &&
        not (LibraryFunctions.is_safe_uncalled fn.vname) &&
        not (Cil.hasAttribute "goblint_stub" fn.vattr)
      in
      let print_and_calculate_uncalled = function
        | GFun (fn, loc) when is_bad_uncalled fn.svar loc->
          let cnt = Cilfacade.countLoc fn in
          uncalled_dead := !uncalled_dead + cnt;
          if get_bool "ana.dead-code.functions" then
            M.warn ~loc:(CilLocation loc) ~category:Deadcode "Function '%a' is uncalled: %d LLoC" CilType.Fundec.pretty fn cnt  (* CilLocation is fine because always printed from scratch *)
        | _ -> ()
      in
      List.iter print_and_calculate_uncalled file.globals;

      (* check for dead code at the last state: *)
      let main_sol = try LHT.find lh (List.hd startvars') with Not_found -> Spec.D.bot () in
      if Spec.D.is_bot main_sol then
        M.warn_noloc ~category:Deadcode "Function 'main' does not return";

      if get_bool "dump_globs" then
        I.print_globals gh;

      (**)
      (* AnalysisState.should_warn := true; (* reset for postsolver *) *)
      (* AnalysisState.postsolving := true; *)
      (* (* postsolver *) *)
      (**)
      (* let rho,tau = check entrystates entrystates_global startvars' in *)
      (* let lh, gh = LHT.of_seq rho, GHT.of_seq tau in *)

      if get_string "comparesolver" <> "" then (
        if M.tracing then M.trace "comparesolver" "here";
        let compare_with solve2 =
          let rho, tau = solve2 entrystates entrystates_global startvars' in
          let lh2, gh2 = LHT.of_seq rho, GHT.of_seq tau in
          CompareGlobSys.compare (get_string "solver", get_string "comparesolver") (lh,gh) (lh2, gh2)
        in
        let solve2 = if (get_string "comparesolver" = "bu") then BuSolver.solve else
            (if (get_string "comparesolver" = "fwd") then FwdSolver.solve else WBuSolver.solve) in
        compare_with solve2;
      );

      lh, gh
    in

    (* Use "normal" constraint solving *)
    let timeout_reached () =
      M.error "Timeout reached!";
      (* let module S = Generic.SolverStats (EQSys) (LHT) in *)
      (* Can't call Generic.SolverStats...print_stats :(
         print_stats is triggered by dbg.solver-signal, so we send that signal to ourself in maingoblint before re-raising Timeout.
         The alternative would be to catch the below Timeout, print_stats and re-raise in each solver (or include it in some functor above them). *)
      raise Timeout.Timeout
    in
    let timeout = get_string "dbg.timeout" |> TimeUtil.seconds_of_duration_string in
    let lh, gh = Timeout.wrap solve_and_postprocess () (float_of_int timeout) timeout_reached in
    let module Query = struct
      let ask_global (gh: EQSys.G.t GHT.t) =
        (* copied from Control for WarnGlobal *)
        (* build a man for using the query system *)
        let rec man =
          { ask    = (fun (type a) (q: a Queries.t) -> Spec.query man q)
          ; emit   = (fun _ -> failwith "Cannot \"emit\" in query context.")
          ; node   = MyCFG.dummy_node (* TODO maybe ask should take a node (which could be used here) instead of a location *)
          ; prev_node = MyCFG.dummy_node
          ; control_context = (fun () -> man_failwith "No context in query context.")
          ; context = (fun () -> man_failwith "No context in query context.")
          ; edge    = MyCFG.Skip
          ; local  = Spec.startstate GoblintCil.dummyFunDec.svar (* bot and top both silently raise and catch Deadcode in DeadcodeLifter *) (* TODO: is this startstate bad? *)
          ; global = (fun v -> EQSys.G.spec (try GHT.find gh (EQSys.GVar.spec v) with Not_found -> EQSys.G.bot ())) (* TODO: how can be missing? *)
          ; spawn  = (fun ?(multiple=false) v d   -> failwith "Cannot \"spawn\" in query context.")
          ; split  = (fun d es   -> failwith "Cannot \"split\" in query context.")
          ; sideg  = (fun v g    -> failwith "Cannot \"split\" in query context.")
          }
        in
        Spec.query man
    end
    in

    let local_xml = solver2source_result lh in
    current_node_state_json := (fun node -> Option.map LT.to_yojson (Result.find_option local_xml node));


    let liveness =
      if get_bool "ana.dead-code.lines" || get_bool "ana.dead-code.branches" then
        print_dead_code local_xml !uncalled_dead
      else
        fun _ -> true (* TODO: warn about conflicting options *)
    in

    if get_bool "exp.cfgdot" then
      CfgTools.dead_code_cfg ~path:(Fpath.v "cfgs") (module FileCfg) liveness;

    let warn_global g v =
      (* Logs.debug "warn_global %a %a" EQSys.GVar.pretty_trace g EQSys.G.pretty v; *)
      match g with
      | `Left g -> (* Spec global *)
        Query.ask_global gh (WarnGlobal (Obj.repr g))
      | `Middle _ (* return global *)
      | `Right _ ->
        ()
    in
    Timing.wrap "warn_global" (GHT.iter warn_global) gh;


    if get_bool "exp.arg.enabled" then ( failwith "no_arg" );

    (* Before SV-COMP, so result can depend on YAML witness validation. *)
    let yaml_validate_result =

      None
    in


    let marshal = Spec.finalize () in
    (* copied from solve_and_postprocess *)
    let gobview = get_bool "gobview" in
    let save_run_str = let o = get_string "save_run" in if o = "" then (if gobview then "run" else "") else o in
    if save_run_str <> "" then (
      if M.tracing then M.trace "marshal" "marshalling";
      let save_run = Fpath.v save_run_str in
      let analyses = Fpath.(save_run / "analyses.marshalled") in
      let config = Fpath.(save_run / "config.json") in
      let meta = Fpath.(save_run / "meta.json") in
      let solver_stats = Fpath.(save_run / "solver_stats.csv") in (* see Generic.SolverStats... *)
      let cil = Fpath.(save_run / "cil.marshalled") in
      let warnings = Fpath.(save_run / "warnings.marshalled") in
      let stats = Fpath.(save_run / "stats.marshalled") in
      Logs.Format.debug "Saving the current configuration to %a, meta-data about this run to %a, and solver statistics to %a" Fpath.pp config Fpath.pp meta Fpath.pp solver_stats;
      GobSys.mkdir_or_exists save_run;
      GobConfig.write_file config;
      Serialize.marshal marshal Fpath.(save_run / "spec_marshal")
    );
    if get_bool "incremental.save" then (
      Serialize.Cache.(update_data AnalysisData marshal);
      if not (get_bool "server.enabled") then
        Serialize.Cache.store_data ()
    );
    if get_string "result" <> "none" then Logs.debug "Generating output: %s" (get_string "result");

    Messages.finalize ();
    Timing.wrap "result output" (ResultOutput.output (lazy local_xml) liveness gh I.make_global_fast_xml) (module FileCfg)
end

let make_a (module CFG : CfgBidirSkip) (module Spec : Spec') file fs change_info =
  let module A = AnalyzeCFG (CFG) (Spec) (struct let increment = change_info end) in
  GobConfig.with_immutable_conf (fun () -> A.analyze file fs)

let analyze_loop = CommonControl.make_analyze_loop get_spec make_a

(** The main function to perform the selected analyses. *)
let analyze change_info file fs = CommonControl.analyze analyze_loop change_info file fs
