open GobConfig

exception RestartAnalysis
exception RestartTimeout

let setConf () =
  let conflist = get_string_list "restart.conflist" in
  let restart = get_bool "restart.enabled" in
  set_conf Options.defaults;
  set_bool "restart.enabled" restart;
  match conflist with 
  | [] -> failwith "empty conflist"
  | [x] -> set_bool "restart.enabled" false; merge_file (Fpath.v x); write_file (Fpath.v "testConf2"); Logs.debug "CONFLIST: %s" x
  | x::xs -> merge_file (Fpath.v x); write_file (Fpath.v "testConf1"); List.iter (set_string "restart.conflist[+]") xs; Logs.debug "CONFLIST: %s" x

(*let invariant_parser: WitnessUtil.InvariantParser.t ResettableLazy.t =
  ResettableLazy.from_fun (fun () ->
      WitnessUtil.InvariantParser.create !Cilfacade.current_file
    )

  let arg_wrapper: (module Server.ArgWrapper) ResettableLazy.t =
  ResettableLazy.from_fun (fun () ->
      let module Arg = (val (Option.get_exn !ArgTools.current_arg Response.Error.(E (make ~code:RequestFailed ~message:"not analyzed or arg disabled" ())))) in
      let module Locator = WitnessUtil.Locator (Arg.Node) in
      let module StringH = Hashtbl.Make (Printable.Strings) in

      let locator = Locator.create () in
      let ids = StringH.create 113 in
      let cfg_nodes = StringH.create 113 in
      Arg.iter_nodes (fun n ->
          let cfgnode = Arg.Node.cfgnode n in
          let loc = UpdateCil.getLoc cfgnode in
          if is_server_node cfgnode then
            Locator.add locator loc n;
          StringH.replace ids (Arg.Node.to_string n) n;
          StringH.add cfg_nodes (Node.show_id cfgnode) n (* add for find_all *)
        );

      let module ArgWrapper =
      struct
        module Arg = Arg
        module Locator = Locator
        let locator = locator
        let find_node = StringH.find ids
        let find_cfg_node = StringH.find_all cfg_nodes
      end
      in
      (module ArgWrapper: ArgWrapper)
    )*)
(*
module Locator = WitnessUtil.Locator (Node)
let node_locator: Locator.t ResettableLazy.t =
  ResettableLazy.from_fun (fun () ->
      let module Cfg = (val !MyCFG.current_cfg) in
      let locator = Locator.create () in

      (* DFS, copied from CfgTools.find_backwards_reachable *)
      let module NH = MyCFG.NodeH in
      let reachable = NH.create 100 in
      let rec iter_node node =
        if not (NH.mem reachable node) then begin
          NH.replace reachable node ();
          List.iter (fun (_, prev_node) ->
              iter_node prev_node
            ) (Cfg.prev node)
        end
      in

      Cil.iterGlobals !Cilfacade.current_file (function
          | GFun (fd, _) ->
            let return_node = Node.Function fd in
            iter_node return_node
          | _ -> ()
        );

      locator
    )
      *)

let reset_states () =
  Messages.Table.(MH.clear messages_table);
  Messages.(Table.MH.clear final_table);
  Messages.Table.messages_list := [];
  Serialize.Cache.reset_data SolverData;
  Serialize.Cache.reset_data AnalysisData;

  (*ResettableLazy.reset Server.invariant_parser;
    ResettableLazy.reset Server.arg_wrapper;*)

  InvariantCil.reset_lazy ();
  Cilfacade.reset_lazy ();
  InvariantCil.reset_lazy ();
  WideningThresholds.reset_lazy ();
  IntDomain.reset_lazy ();
  FloatDomain.reset_lazy ();
  StringDomain.reset_lazy ();
  PrecisionUtil.reset_lazy ();
  ApronDomain.reset_lazy ();
  AutoTune.reset_lazy ();
  LibraryFunctions.reset_lazy ();
  Access.reset ();

  (*
  if get_bool "dbg.timing.enabled" then (
    Logs.newline ();
    Goblint_solver.SolverStats.print ();
    Logs.newline ();
    Logs.info "Timings:";
    Timing.Default.print (Stdlib.Format.formatter_of_out_channel @@ Messages.get_out "timing" Batteries.Legacy.stderr);
    flush_all ()
  );

  Goblint_solver.SolverStats.reset ();
  Timing.Default.reset ();
  Timing.Program.reset ();

  if get_bool "dbg.timing.enabled" then (
    let tef_filename = get_string "dbg.timing.tef" in
    if tef_filename <> "" then
      Goblint_timing.setup_tef tef_filename;
    Timing.Default.start {
      cputime = true;
      walltime = true;
      allocated = true;
      count = true;
      tef = true;
    };
    Timing.Program.start {
      cputime = false;
      walltime = false;
      allocated = false;
      count = false;
      tef = true;
    }
  );*)
