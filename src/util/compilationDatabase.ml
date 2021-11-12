open Prelude

let preprocess ~custom_include_dirs () =
  let cd = Yojson.Safe.from_file "compile_commands.json" in
  let open Yojson.Safe.Util in
  let i = ref 0 in
  convert_each (fun entry ->
      let file = entry |> member "file" |> to_string in
      (* let command = entry |> member "command" |> to_string in *)
      let arguments = entry |> member "arguments" |> convert_each to_string in
      let o_re = Str.regexp "-o +[^ ]+" in
      let file' = Printf.sprintf "%d.i" !i in
      (* let command' = Str.replace_first o_re (cppflags ^ " -E -o " ^ file') command in *)

      let (o_i, _) = List.findi (fun i e -> e = "-o") arguments in
      let (arguments_init, _ :: _ :: arguments_tl) = List.split_at o_i arguments in
      let arguments' = arguments_init @ "-I" :: List.hd custom_include_dirs :: "-E" :: "-o" :: file' :: arguments_tl in (* TODO: custom includes, cppflags *)
      let command' = Filename.quote_command (List.hd arguments') (List.tl arguments') in

      Printf.printf "CD: %s: %s\n" file command';
      ignore (Sys.command command');
      incr i;
      file'
    ) cd
