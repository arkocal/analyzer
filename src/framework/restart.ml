open GobConfig

exception RestartAnalysis
exception RestartTimeout

(*consider not defaulting all conf settings*)
let setConf () =
  let conflist = get_string_list "restart.conflist" in
  let restart = get_bool "restart.enabled" in
  set_conf Options.defaults;
  set_bool "restart.enabled" restart;
  match conflist with 
  | [] -> failwith "empty conflist"
  | [x] -> merge_file (Fpath.v x); set_bool "restart.enabled" false; Logs.debug "CONFLIST: %s" x
  | x::xs -> merge_file (Fpath.v x); List.iter (set_string "restart.conflist[+]") xs; Logs.debug "CONFLIST: %s" x

(*copied from server.ml analyze. Consider copying over node_locator, arg_wrapper and invariant_parser and also resetting
  maingoblint timer stats
  Example call that runs slower: ./goblint -v --conflist conf/examples/large-program.json --conflist conf/examples/very-precise.json ../bench/pthread/zfs-fuse_comb.c
  Example call that crashes on restart: ./goblint -v --conflist conf/examples/large-program.json --conflist conf/examples/very-precise.json ../bench/coreutils/cksum_comb.c 
*)
let reset_states () =
  Messages.Table.(MH.clear messages_table);
  Messages.(Table.MH.clear final_table);
  Messages.Table.messages_list := [];
  Serialize.Cache.reset_data SolverData;
  Serialize.Cache.reset_data AnalysisData;
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
