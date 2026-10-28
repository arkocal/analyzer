# Garbled message output after `Domain.spawn` (Batteries + OCaml 5 `Format`)

## Symptom
With the parallel solvers and ≥2 domains, multi-line messages lose their line breaks:
indentation gets printed first and the text comes out glued together, e.g.
`[Warning][Race] Memory location …:write with …` and `race summary:safe: 2vulnerable: 0`.
It's deterministic, and it happens even with `warn_at=post`, where all printing is done by the main domain after solving.
Sequential solvers and runs with 1 domain are not affected.

## Cause
Two global modifications of `Format.std_formatter` interact:

1. **Batteries:** when Goblint starts, `BatFormat`'s initializer (it is linked into goblint) runs
   `pp_set_formatter_output Format.std_formatter BatIO.stdout`. This replaces **all** of the formatter's output functions,
   including `out_newline`/`out_spaces`/`out_indent`, with functions that write directly to stdout. It does the same to `err_formatter`.
2. **OCaml 5** (`format.ml`, `Domain.before_first_spawn`): on the first spawn, it moves **only**
   `out_string`/`out_flush` into a domain-local buffer.

After the first spawn, text sits in the buffer while newlines and indentation go straight to stdout, so they come out
before the text. With the plain stdlib this doesn't happen, because stdlib's `out_newline` writes through the current `out_string`.

Minimal repro (one spawn, then print a `@[<v 2>…@,…@]` box):
```
stdlib only:        [Warning] header:          Batteries linked:   (newline + indents)
                      piece one                                    [Warning] header:piece onepiece two
                      piece two
```

## Fix on `td_parallel_pr`
Replaces Felix's "Make messages thread-safe" (`module Format = BatFormat` plus workarounds for `dprintf`/`pp_print_option`)
with a one-line change in `messages.ml`:
```ocaml
let formatter = ref BatFormat.std_formatter   (* and the matching `==` check in the AfterConfig colour hook *)
```
`BatFormat.std_formatter` is a separate formatter that the spawn hook never touches. Tested with both parallel solvers,
2 and 4 domains, and `warn_at` post and early: no garbled output, and in post mode the output is byte-identical to Felix's version.
Note that neither version makes messages thread-safe (`Messages.Table` and the formatter are still unsynchronized).

## Open points for discussion
- **`messagesCompare.ml:27`** sets colour tags on `Format.std_formatter`, assuming that is the formatter `Messages` uses.
  It should be `!Messages.formatter` (this was already broken under Felix's version).
- **`Logs`** still prints through the stdlib `std_formatter`/`err_formatter` that Batteries modified. It works today only because each log
  line is plain text ending in `"\n%!"`. A `%a` printer with Format boxes/break hints would garble the same way after a spawn.
- **Broader fix?** We could point the stdlib formatters back at their original output functions after Batteries initializes (which covers `Logs`
  and everything else), or report this upstream to Batteries. Its initializer should probably leave `out_newline`/`out_spaces`/`out_indent`
  alone, or set them via `out_string`, so that they stay consistent with OCaml 5's hook.
