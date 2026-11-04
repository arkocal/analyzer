# `wbu_iter`: non-recursive WBU solver

`wbu_iter` (`src/solver/wbu_iter.ml`) follows the same evaluation order as `wbu`,
but keeps what would be on the call stack in per-unknown frames. It has been
measured at commit `cff0bb08e`. No code outside the solver changed, apart from
the wiring that selects it with `--set solver wbu_iter`.

Across the four programs measured, the results are identical to `wbu`, and `wbu_iter`
uses 29–82% fewer instructions and 19–68% less peak memory. Most of the saving comes
from global update lookups. RHS evaluation is now 59–70% of the runtime.

## How it works

- Each unknown has a frame holding `todos`, the contributions it still has to
  make, and `finisher`, the frame to return to when it is done.
- At most one frame is active. When its todos are done, the solver returns to the
  finisher. If there is none, it takes the next unknown from the worklist.
- `run` is a tail-recursive loop, so the OCaml stack stays flat during solving.

### Differences from `wbu` that affect performance

| Change | Effect |
|---|---|
| Frames hold the `Lcl` record | No repeated lookups of the same unknown |
| Each global contribution keeps a map from unknown to its newest value, instead of the `last` table plus a set of unknowns | No per-global hashtable, and no lookup per contributor when joining. `wbu`'s `Hashtbl.add` on `last` also keeps old bindings |
| Skip the join if an unknown contributes the same value again | Avoids re-joining all values at that origin |
| `infl` is keyed by integer frame id | Adding an influence does not compare unknowns |

`infl` is visited in id order. That makes no difference for the `set` worklist (the
default), but `lifo` can differ from `wbu`, so `lifo` is excluded from the comparison.

## Correctness

**Small inputs:** all 180 jobs passed: 18 regression tests on the 8 `wbu_*_set`
configs in `conf/forward_artifact`, plus 36 with `solvers.bu.always_warrow`.

- RHS and TF counts match.
- The output is identical.
- `comparesolver` finds equal values in both directions.
- No fixpoint errors.

**Benchmarks:** on all four programs below, RHS and TF match and the output is
identical, ignoring the solver timestamps and timing statistics. Neither solver
reported fixpoint errors. Both report `SV-COMP result: true`, except on `pingfs`,
where both report `unknown`.

**Stack:** on one constructed 30000-statement program under `ulimit -s` 4 MB, `wbu`
overflowed the stack and `wbu_iter` finished.

## Benchmarks

### Setup

- `wbu_case_study` artifact configs, in this order: `bench/conf/level01.json`,
  `bench/conf/casestudy.json`, `solver-configs/wbu_digest_gc_abort_set.json`.
- `--set ana.specification bench/no-data-race.prp`, then `--set solver <s>`, which must
  come after the confs.
- Release build. Hash consing is on, as it is by default.
- `uthash` and `pingfs` come from the artifact's `bench/concrat`. `ipmi` and `vhost` come
  from `sv-benchmarks/c/ldv-linux-3.0`: `module_get_put-drivers-char-ipmi-ipmi_watchdog`
  and `usb_urb-drivers-vhost-vhost_net`.

There is one run per solver per program, so times are rough. Instruction counts are the
more reliable figure: repeated runs on small inputs varied by under 0.1%.

### Results

| Program | RHS | TF | Solver time (s) | Wall time (s) | Instructions (G) | Peak RSS (MB) |
|---|---:|---:|---:|---:|---:|---:|
| `pingfs` | 168808 | 168551 | 82.3 → 49.0 (−40.5%) | 99.8 → 67.9 | 566.9 → 370.9 (−34.6%) | 539 → 384 (−28.7%) |
| `ipmi` | 323968 | 245658 | 99.5 → 63.4 (−36.3%) | 106.5 → 70.6 | 510.2 → 362.2 (−29.0%) | 456 → 367 (−19.6%) |
| `uthash` | 474146 | 637123 | 467.8 → 172.2 (−63.2%) | 492.8 → 201.0 | 2053 → 1014 (−50.6%) | 1387 → 444 (−68.0%) |
| `vhost` | 816379 | 797192 | 2341.9 → 215.0 (**−90.8%**) | 2360.6 → 233.9 | 7515 → 1381 (−81.6%) | 1525 → 1234 (−19.1%) |

Each cell reads `wbu` → `wbu_iter`. Each pair ran side by side, except on `uthash`, where
the `wbu` run is from an earlier batch than the `wbu_iter` run, so its time comparison is
less exact.

| Program | Minor words (G) | Promoted words (M) | Top heap words (M) | Major collections |
|---|---:|---:|---:|---:|
| `pingfs` | 39.2 → 27.7 | 286.6 → 269.2 | 69.2 → 45.5 | 49 → 61 |
| `ipmi` | 22.0 → 18.4 | 222.4 → 211.3 | 58.8 → 44.5 | 40 → 43 |
| `uthash` | 176.9 → 76.0 | 1001 → 716 | 179.0 → 50.9 | 85 → 144 |
| `vhost` | 133.8 → 67.9 | 669.0 → 628.9 | 204.2 → 154.4 | 48 → 52 |

### Where the time goes

Shares of a call-graph profile at 29 Hz, `wbu` → `wbu_iter`. The rows with few samples
are rough.

| Row | `pingfs` | `ipmi` | `vhost` |
|---|---:|---:|---:|
| RHS evaluation | 40.4% → 58.8% | 41.4% → 63.2% | 6.9% → 70.3% |
| Post-solve check | 17.5% → 26.4% | 6.3% → 10.2% | 0.8% → 8.0% |
| Global update, total | 31.2% → 3.9% | 35.5% → 4.1% | 90.4% → 4.3% |
| – lookups | 27.7% → 0.5% | 33.3% → 1.7% | 89.7% → 0.4% |
| Local update | 5.2% → 6.8% | 11.7% → 16.6% | 1.5% → 14.3% |
| Global read | 3.8% → 2.1% | 3.1% → 4.0% | 0.1% → 1.2% |
| Everything else | 2.0% → 2.0% | 2.0% → 1.9% | 0.3% → 1.9% |

The same split in seconds for `vhost`, with total runtimes of 2328 s and 222 s:

| Row | `wbu` | `wbu_iter` |
|---|---:|---:|
| Global update, total | ~2105 s | ~9.5 s |
| – lookups | ~2087 s | ~1 s |
| RHS evaluation | ~161 s | ~156 s |
| Local update | ~34 s | ~32 s |
| Post-solve check | ~18 s | ~18 s |
| Global read | ~3 s | ~3 s |
| Everything else | ~6 s | ~4 s |

On `uthash`, `wbu_iter` splits into RHS 62.1%, post-solve 16.8%, local update 9.8%, global
update 5.7%, global read 3.7% and the rest 2%. `wbu` spends about 46% of its time in
global update there, about 36% of its time in lookups.

### GC

Shares of a flat profile, `wbu` → `wbu_iter`.

| Program | GC total | Stack scan (`caml_oldify_local_roots`) |
|---|---:|---:|
| `pingfs` | 6.5% → 9.2% | 0.51% → 0.46% |
| `ipmi` | 4.9% → 6.5% | 0.52% → 0.34% |
| `uthash` | 6.5% → 8.7% | 1.43% → 0.49% |
| `vhost` | 0.7% → 6.1% | 0.11% → 0.42% |

GC's share rises because the total shrinks. On `vhost`, its absolute time is about the
same: ~16 s against ~14 s. Stack scanning never exceeds 1.43%, so removing recursion is
not what makes `wbu_iter` faster. The saving comes from removing work per contribution.

### Effect of each step on `uthash`

| Commit | Change | Instructions (G) | Peak RSS (MB) |
|---|---|---:|---:|
| baseline | `wbu` | 2053 | 1387 |
| `7ee1fa44f` | Remove redundant lookups | 1311 | 408 |
| `b8180dff5` | Skip re-joining unchanged values | 1205 | 408 |
| `cff0bb08e` | Key `infl` by frame id | 1014 | 444 |

The RSS differences between the last three rows may just be GC timing, since their top
heap sizes are identical.

## Conclusions

- **The gain varies a lot between programs.** `wbu`'s runtime depends heavily on its global
  update lookups. `uthash` and `vhost` take a similar time with `wbu_iter` (201 s and 234 s),
  but about 5× apart with `wbu` (493 s and 2361 s).
- **Speed and memory gains are separate.** `vhost` has the largest speedup and the smallest
  memory saving, while `uthash` has the largest memory saving.
- **`vhost`'s time gain exceeds its instruction gain** (−90.8% against −81.6%), so `wbu` also
  gets less done per instruction. Cache misses in long hash buckets would explain this, but
  they were not measured.
- **RHS count is now the main cost.** No other row is above 26%, and the largest of those is
  the post-solve check, which is not part of the solving itself.

## Open points

- A second join shortcut, for when a contribution only grows.
- The order of global todos, and dropping them on abort.
- Coalescing evaluations, to reduce the RHS count.
- Outside the solver: `binop_compare` in `mCPRegistry.ml` rebuilds `domain_list ()` on every
  digest comparison, which is worth raising with its owner.
- Older artifact results on `uthash` show far fewer RHS evaluations than the 474146 here: TD
  at 41787 and `wbu` without digests at 17326. They came from an older build and have not
  been rerun.
