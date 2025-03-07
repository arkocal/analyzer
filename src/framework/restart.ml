open GobConfig

exception RestartAnalysis
exception RestartTimeout

let set_conflist l =
  match l with
  | [] -> ()
  | x::xs -> set_string "restart.conflist[+]" x

let setConf () =
  let conflist = get_string_list "restart.conflist" in
  let restart = get_bool "restart.enabled" in
  set_conf Options.defaults;
  set_bool "restart.enabled" restart;
  match conflist with 
  | [] -> failwith "empty conflist"
  | [x] -> set_bool "restart.enabled" false; merge_file (Fpath.v x); Logs.debug "conflist: %s" x
  | x::xs -> set_conflist xs; merge_file (Fpath.v x); Logs.debug "conflist: %s" x

let reset_states () =
  Messages.Table.(MH.clear messages_table);
  Messages.(Table.MH.clear final_table);
  Messages.Table.messages_list := [];
  Serialize.Cache.reset_data SolverData;
  Serialize.Cache.reset_data AnalysisData;
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