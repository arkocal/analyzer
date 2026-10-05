# td3 vs wbu on rsync (AnalyzeThat 2026)

**Why:** compare the runtime of the td3 and wbu solvers on a real program: rsync v3.4.1, as set up in
[goblint/bench `analyzethat2026`](https://github.com/goblint/bench/tree/analyzethat2026/analyzethat2026).
Only runtimes are compared, not precision.

## Run

`bench/` has the rsync input and the bench configs (see `bench/SOURCE.md`). Goblint is the build of
this repo (`../goblint`, set in `config.yaml`):

```sh
make release                 # in the analyzer root, after make setup
cd td3_vs_wbu_rsync
uv run snakemake             # settings from profiles/default: 2 cores, keep-going, rerun only on missing output
```

Results show up as each run terminates:
- `results/results.csv`: one row appended per finished run, in any order.
- `results/compare.csv`: written at the end, with one row per config showing td3 and wbu side by side.
- `out/<config>/<solver>/goblint.log` and `result.json`: per-run details.

## What runs

- **Matrix:** the bench configs listed in `config.yaml`, each run with each solver.
- **Solver settings:** applied after the bench config, from `solver-configs/<solver>.json`.
  - `td3.json` only sets the solver.
  - `wbu.json` sets the fwd/bu options of the forward artifact (`wbu_digest_gc_abort_set`).
  - The widening gas (`solvers.td3.*`) is not overridden, so both solvers use the defaults.
- **Input:** `rsync-v3.4.1-gcc-nosignal.i`.
- **Not run:** the call-string configs (`jprotopopov*`) and the earlyglobs-off config, which ran out of memory at 30 GB.
- **Long configs:** the `sim642-minimal*` configs other than `minimal3` take 7 h or more with td3, and are commented out.

## Limits and status

Set in `config.yaml` (0 = no limit):
- `timeout_s`: no limit by default.
- `memory_limit_mb`: 14 GB, enforced with `RLIMIT_AS`.

`status` in the results is one of:
- `done`
- `timeout`
- `oom`
- `error`: the log has the exception
- `signal N`

None of these fail the workflow.

## Notes

- **Don't rebuild goblint during a run.** It is used in place. Every row records the goblint version.
- **Timings are noisier with 2 parallel runs.** They compete for memory bandwidth. For cleaner timings, use `uv run snakemake --cores 1`.
- **Delete `out/<config>/<solver>/` to re-run one combination.** Snakemake re-runs only missing outputs (`rerun-triggers: mtime`), so changing the Snakefile or a config won't throw away hours of results.
- **wbu solver time vs. wall time.** `solver_s` (wbu only) is the solver time from wbu's `Solver start/end` log lines. `wall_s` is the whole goblint process, including parsing and postsolving, and is the number to compare.
