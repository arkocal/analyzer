# conc-all benchmark, 2026-10-30: td_parallel_immediate vs td3

## Setup
- Build: `heads/svcomp-parallel-wip-0-g08a487a2f` on a freshly rebuilt opam switch (OCaml 5.4.1+flambda). `goblint-cil` is pinned to the local `~/Workspace/cil`, which has the `Domain.DLS` alignment fix.
- Selection: `conc-all`, the whole of C.Concurrency (3274 runs).
- Tools (sv-easy):
  - `l1noct-*`: common + verify + `level01-nooct`, i.e. level01 without autotune octagon, with congruence on.
  - `l0-*`: common + verify + `level00`.
  - `*-immed` adds `parallel.json`: td_parallel_immediate, 4 domains, saturn hashmap, hashcons on (GobHashcons spinlock version), ARG off.
- No Apron in any of these runs: neither level activates an Apron analysis.
- Limits:
  - immed: `q60-par4`, which is 60s CPU, 75s hardtime, 4GB, 8 vCPUs (4 physical cores), 2 runs in parallel.
  - td3: `q60-p4`, which is 60s CPU, 75s hardtime, 2GB, 1 core, 4 runs in parallel.
  - The CPU time is summed over all cores, as in SV-Comp.
- Results: `~/Workspace/sv-easy/results/conc-all/{l1noct-immed/2026-10-30_135608, l1noct-td3/2026-10-30_141422, l0-immed/2026-10-30_142804, l0-td3/2026-10-30_145209}`.
- **l0-td3 is incomplete.** BenchExec hung at 14:56 with no goblint process running, the same hang as on 10-28. That leaves 1151 of the 3274 runs, so the l0 numbers below only cover those 1151.

## Soundness / verdicts

| | correct | wrong | verify errors | other errors |
|---|---|---|---|---|
| l1noct-immed (today) | 1331 | 0 | 0 | 20 |
| l1noct-td3 (today) | 1329 | 0 | 0 | 23 |
| l1noct-immed, 10-28 (30s, cde77826f, hashcons off) | 1321 | 0 | 10 | 41 (incl. 1 segfault) |
| l0-immed / l0-td3 (1151 common runs) | 716 / 716 | 0 / 0 | 0 / 0 | 7 / 7 |

Notes on the table:
- **Errors in l1noct-immed (20):** 7× TIMEOUT, 6× `Cilfacade.TypeOfError` (`per-thread-array-join-counter*`), 4× OOM (`thread-join-binomial*`), 3× `Failure` (`nsc-ircc`). Apart from the timeouts these are the same failures td3 has.
- **Immediate vs td3:** no task is correct only with td3. The 2 runs correct only with immediate are both `04-mutex_05-lockfuns`, which immediate solves in 0.1s while td3 times out (the known td3 non-termination).
- **Compared with 10-28:** all 10 earlier verify errors (mostly pthread-wmm, e.g. mix018_tso, mix032, plus 13-privatized_27) and the segfault are gone, and nothing regressed.
- **The 10-28 18:33 run (spinlock build):** it failed almost every task after about 0.06s with `Fun.Finally_raised` or `FrontendError`, so that build or environment was broken. It doesn't happen today.

## CPU time vs walltime

Totals over all runs:

| | CPU | walltime | CPU / wall |
|---|---|---|---|
| l1noct-immed | 3307s | 1664s | 1.99 |
| l1noct-td3 | 2562s | 2569s | 1.00 |
| l0-immed (1151 runs) | 812s | 460s | 1.76 |
| l0-td3 (1151 runs) | 664s | 668s | 0.99 |

The two tables below only count tasks correct in both runs, grouped by how long td3 takes. Columns:
- "wall speedup" is td3 walltime / immed walltime.
- "CPU overhead" is immed CPU / td3 CPU.
- "cores used" is immed CPU / immed walltime.
- The median column is the median of per-task walltime speedups.

**l1noct** (1329 tasks):

| td3 walltime | n | walltime immed / td3 | wall speedup | CPU immed / td3 | CPU overhead | cores used | median per-task speedup |
|---|---|---|---|---|---|---|---|
| < 0.5s | 1082 | 220 / 238s | 1.08× | 373 / 236s | 1.58× | 1.70 | 1.08 |
| 0.5–2s | 227 | 116 / 181s | 1.56× | 213 / 180s | 1.18× | 1.84 | 1.91 |
| 2–10s | 19 | 44 / 58s | 1.32× | 93 / 58s | 1.60× | 2.12 | 1.48 |
| > 10s | 1 | 17 / 25s | 1.45× | 49 / 25s | 1.98× | 2.87 | 1.45 |

**l0** (716 tasks, partial):

| td3 walltime | n | walltime immed / td3 | wall speedup | CPU immed / td3 | CPU overhead | cores used | median per-task speedup |
|---|---|---|---|---|---|---|---|
| < 0.5s | 499 | 106 / 124s | 1.17× | 172 / 123s | 1.40× | 1.62 | 1.28 |
| 0.5–2s | 199 | 104 / 152s | 1.47× | 189 / 151s | 1.25× | 1.82 | 1.70 |
| 2–10s | 18 | 38 / 62s | 1.61× | 72 / 61s | 1.18× | 1.89 | 1.48 |

Largest tasks in l1noct (by td3 walltime):

| task | td3 walltime | immed walltime | immed CPU |
|---|---|---|---|
| ldv-linux-3.14-races/…spi-tegra20-slink | 24.6s | 16.9s | 48.7s |
| goblint-regression/28-race_reach_73-funloop_hard_racefree | 6.2s | 4.9s | 12.0s |
| ldv-linux-3.14-races/…usb–misc–adutux | 5.3s | 5.2s | 11.7s |
| pthread-ext/28_buggy_simple_loop1_vf | 5.0s | 3.2s | 7.6s |
| goblint-regression/28-race_reach_71-funloop_racing | 3.6s | 3.4s | 9.2s |
| goblint-regression/28-race_reach_72-funloop_hard_racing | 3.6s | 4.4s | 9.3s |

## Takeaways
- Immediate was sound on all of conc-all at l1noct (congruence on) with GobHashcons: no wrong verdicts and no verify errors.
- **Parallelism is low.** On average only about 1.7–2 of the 4 domains are busy.
- **The walltime gain is modest:** 1.1–1.6×.
- **The CPU overhead is larger than the gain:** 1.2–1.6×, and up to about 2× on the largest task. SV-Comp limits CPU time summed over cores, so at equal CPU time immediate gets less work done than td3. Here that cost no task, but it would matter at the 15-minute limit.
- 80% of conc tasks take td3 under 0.5s, where parallel solving can only add overhead. The likely suspects are fixed costs: domain startup, the Saturn table and the sharded hashcons. Profile one tiny task to find out.

## Open
- Finish l0-td3. BenchExec hangs reproducibly on conc-all, even with only 4 parallel runs. A watchdog in the bench script could detect a log that stops changing and restart.
- Profile the fixed overhead on a tiny task.
- Run the td3 sequential cost of GobHashcons (`l0-td3` vs `l0-td3-nohc`).
