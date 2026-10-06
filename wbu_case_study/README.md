# wbu case study on Concrat programs

**Why:** a deliberately cherry-picked case study for the wbu paper (the fair benchmark has already
been run). It shows two things on real programs:

1. **History tokens vs. the cardinal power construction:** wbu with `solvers.fwd.digests` on vs. off.
2. **wbu vs. the paper's TD baseline** (`td_simplified_ref_improved`). The solver parameters are kept
   consistent with that solver. td3 is *not* a comparable baseline and is not run here.

The config makes the analysis path-sensitive enough that history tokens actually do work: the wbu
paper's `level01`, plus the path-sensitive analyses `malloc_null`, `memLeak` and `uninit`. The
heavier levels don't work for this: `branchSet` (level03) explodes on every real program, and
level02 crashes on sshfs. See `experiment-results/case_study_wbu/REPORT.md` in the analyzer repo for
how this config was found.

Reference result (full snoopy, single local runs on E-cores), solver time:
wbu digest 128 s, wbu nodigest 260 s, td_simplified_ref_improved 214 s.

The long-term goal is a program or config where this takes a couple of hours.

## Run

`bench/` has the inputs and configs (see `bench/SOURCE.md`). Goblint is the build of this repo
(`../goblint`, set in `config.yaml`):

```sh
make release                 # in the analyzer root, after make setup
cd wbu_case_study
uv run snakemake             # settings from profiles/default: 15 cores, keep-going, rerun only on missing output
uv run snakemake -n          # dry run: list what would run
```

Results show up as each run finishes:
- `results/results.csv`: one row appended per finished run, in any order.
- `results/compare.csv`: written at the end, with one row per program, all solvers side by side,
  plus `nodigest_over_digest` and `td_over_digest` (solver-time ratios, for runs that finished).
- `out/<program>/<solver>/goblint.log` and `result.json`: per-run details.

## What runs

- **Matrix:** the programs in `config.yaml` × the three solvers.
- **Config order:** `bench/conf/level01.json`, `bench/conf/casestudy.json`, `solver-configs/<solver>.json`,
  then `--set ana.specification bench/no-data-race.prp`. This is the same as the forward artifact
  (level config + solver config + property), with `casestudy.json` in between.
- **Solvers** (`solver-configs/`, copied unchanged from the forward artifact): all three have the same
  widening/narrowing gas (`widen_gas` 10, `side_widen_gas` 10, narrow gas 3).

## Limits and status

Set in `config.yaml` (0 = no limit):
- `timeout_s`: 24 h.
- `memory_limit_mb`: 50 GiB per run, enforced with `RLIMIT_AS`.

`status` in the results is one of `done`, `timeout`, `oom`, `error` (the log has the exception) or
`signal N`. None of these fail the workflow.

## Notes

- **Don't rebuild goblint during a run.** It is used in place. Every row records the goblint version.
- **Other workflows on the same machine** (e.g. `td3_vs_wbu_rsync`) compete for cores and memory
  bandwidth. Check that the cores and memory in `profiles/default` are free.
- **Delete `out/<program>/<solver>/` to re-run one combination.** Snakemake re-runs only missing
  outputs (`rerun-triggers: mtime`).
- **Solver time vs. wall time.** `solver_s` comes from the `Solver start/end` log lines, which both wbu
  and td_simplified_ref_improved write. It excludes parsing and postsolving. It is the number the case
  study compares.
