open GobConfig

exception RestartAnalysis
exception RestartTimeout

let set_conflist l =
  match l with
  | [] -> ()
  | x::xs -> set_string "restart.conflist[+]" x

let setConf () =
  Logs.debug "RESTARTING";
  let conflist = get_string_list "restart.conflist" in
  let restart = get_bool "restart.enabled" in
  set_conf Options.defaults;
  set_bool "restart.enabled" restart;
  match conflist with 
  | [] -> failwith "empty conflist"
  | [x] -> set_bool "restart.enabled" false; merge_file (Fpath.v x); Logs.debug "conflist: %s" x
  | x::xs -> set_conflist xs; merge_file (Fpath.v x); Logs.debug "conflist: %s" x