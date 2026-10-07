# wbu case study on Concrat programs

**Why:** a deliberately cherry-picked case study for the wbu paper (the fair benchmark has already
been run). It shows two things on real programs:

1. **History tokens vs. the cardinal power construction:** wbu with `solvers.fwd.digests` on vs. off.
2. **wbu vs. the paper's TD baseline** (`td_simplified_ref_improved`). The solver parameters are kept
   consistent with that solver. td3 is *not* a comparable baseline and is not run here.

The config makes the analysis path-sensitive enough that history tokens actually do work: the wbu
paper's `level01`, plus the path-sensitive analyses `malloc_null`, `memLeak` and `uninit`. The
heavier levels don't work for this: `branchSet` (level03) explodes on every real program, and
level02 crashes on sshfs. How this config was found is written up in
`experiment-results/case_study_wbu/REPORT.md`, which is only on the original machine (not committed).

Reference result (full snoopy, single local runs on E-cores), solver time:
wbu digest 128 s, wbu nodigest 260 s, td_simplified_ref_improved 214 s.

The long-term goal is a program or config where this takes a couple of hours.

## Status (2026-10-07)

Server run of commit `cb774e0e0`, all 15 jobs started 2026-10-06 15:39:58 in parallel, 24 h timeout
(it ends around 2026-10-07 15:40). Results so far are in `results/results.csv` on the server. Solver
time; "> x" means the run was still going at the newest finished row (2026-10-07 10:20:53):

| Program | wbu digest | wbu nodigest | td_simplified_ref_improved |
|---|---|---|---|
| snoopy | **30.2 s** | 55.5 s | 28.2 s |
| stud | **12446 s (3.5 h)**, 9.5 GB | 67013 s (18.6 h), 7.2 GB | > 18.7 h |
| vanitygen | > 18.7 h | > 18.7 h | **33107 s (9.2 h)**, 10.7 GB |
| wrk | > 18.7 h | > 18.7 h | `signal 6` after 5 h at 44.9 GB |
| sshfs | > 18.7 h | > 18.7 h | > 18.7 h |

What this means:
- **stud is the lead candidate.** History tokens are 5.4× faster than cardinal power, and at least
  5.4× faster than TD. If TD hits the 24 h timeout, the claim is "> 6.9×". The ratio grows with
  program size: it was 1.8–2× on snoopy. stud also meets the "couple of hours" goal.
- **snoopy:** the nodigest/digest ratio is stable (1.84× here, 2.03× locally). The TD comparison is
  not: on the server TD is on par with digest, locally it was 1.7× slower. That is probably
  machine-dependent; TD uses 4.5× more memory. So don't use snoopy for wbu vs. TD.
- **vanitygen is a counter-example:** TD finished at 9.2 h while both wbu runs were still going.
- **wrk TD is probably out of memory, not a crash:** 44.9 GB is close to the 50 GiB `RLIMIT_AS`, and a
  failed allocation can surface as an abort. `run_one.py` only reports `oom` if the log says
  "Out of memory". Check the end of `out/wrk/simplified_ref_improved/goblint.log`.
- Locally (1 h timeout, E-cores), sshfs and stud timed out with all three solvers.

Next steps:
1. After the timeout, read `results/compare.csv` and update the table above.
2. Check the wrk TD log for out-of-memory.
3. Decide how to present vanitygen, and whether stud should be re-run for repetitions (single runs so far).

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
