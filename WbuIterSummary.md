# `wbu_iter` work summary, 2026-11-04

Summary of the work on the `wbu_iterative` branch, with all recorded numbers. It is a
summary, not a transcript. `WbuIter.md` describes the solver and its benchmark setup.

## What was done

1. Designed a non-recursive `wbu`: each unknown gets `todos` and an optional `finisher`,
   with at most one active unknown. When its todos are done, the solver returns to the
   finisher, otherwise it takes the next unknown from the worklist.
2. Implemented it in a new file, `src/solver/wbu_iter.ml`, leaving existing solvers as they
   were (`49ef09724`).
3. Removed redundant lookups inside `wbu_iter` (`7ee1fa44f`).
4. Tried mutable record fields. They gained under 1%, so they were dropped.
5. Added the shortcut for unchanged global values (`b8180dff5`).
6. Keyed `infl` by integer frame id (`cff0bb08e`), and dropped `lifo` from the tests.
7. Compared `wbu` and `wbu_iter` on programs in the 5-minute range, to see if results
   generalize.

RHS count is the main research question. The performance work was to rule out other
blockers. RHS count work has not started.

## Correctness

- Matrix: 180 of 180 jobs passed on RHS and TF counts, identical output, `comparesolver` in
  both directions, and a control run. There were 0 fixpoint errors. It covers 8 `set`
  configs on 18 inputs (144 jobs), plus `always_warrow` on 2 of those configs (36 jobs).
- Constructed 30000-statement program with `ulimit -s` 4 MB: `wbu` overflowed the stack and
  `wbu_iter` finished. The default limit here is 8 MB.

## Comparison at `cff0bb08e`

Config: `level01` + `casestudy` + `wbu_digest_gc_abort_set`, one run per solver.
MB means 1000 KB. Change is from `wbu` to `wbu_iter`.

### `vhost`: RHS 816379, TF 797192, `SV-COMP result: true`

| | `wbu` | `wbu_iter` | Change |
|---|---:|---:|---:|
| Solver time | 2341.9 s | 215.0 s | −90.8% |
| Wall time | 2360.6 s | 233.9 s | −90.1% |
| Instructions | 7515 G | 1381 G | −81.6% |
| Peak RSS | 1525 MB | 1234 MB | −19.1% |
| Top heap words | 204.2 M | 154.4 M | −24.4% |
| Minor words | 133.8 G | 67.9 G | −49.3% |
| Promoted words | 669.0 M | 628.9 M | −6.0% |
| Minor collections | 510353 | 258903 | −49.3% |
| Major collections | 48 | 52 | +8.3% |
| GC share | 0.66% | 6.05% | |
| – minor | 0.26% | 1.96% | |
| – major | 0.40% | 4.09% | |
| Stack scan | 0.11% | 0.42% | |

| Time split | `wbu` (2328 s, 67268 samples) | `wbu_iter` (222 s, 6415 samples) |
|---|---:|---:|
| Global update, total | 90.4%, ~2105 s | 4.3%, ~9.5 s |
| – lookups | 89.7%, ~2087 s | 0.4%, ~1 s |
| RHS evaluation | 6.9%, ~161 s | 70.3%, ~156 s |
| Local update | 1.5%, ~34 s | 14.3%, ~32 s |
| Post-solve check | 0.8%, ~18 s | 8.0%, ~18 s |
| Global read | 0.1%, ~3 s | 1.2%, ~3 s |
| Everything else | 0.3%, ~6 s | 1.9%, ~4 s |

### `ipmi`: RHS 323968, TF 245658, `SV-COMP result: true`

| | `wbu` | `wbu_iter` | Change |
|---|---:|---:|---:|
| Solver time | 99.5 s | 63.4 s | −36.3% |
| Wall time | 106.5 s | 70.6 s | −33.7% |
| Instructions | 510.2 G | 362.2 G | −29.0% |
| Peak RSS | 456.2 MB | 366.9 MB | −19.6% |
| Top heap words | 58.8 M | 44.5 M | −24.4% |
| Minor words | 22.0 G | 18.4 G | −16.4% |
| Promoted words | 222.4 M | 211.3 M | −5.0% |
| Minor collections | 84003 | 70228 | −16.4% |
| Major collections | 40 | 43 | +7.5% |
| GC share | 4.88% | 6.54% | |
| – minor | 1.72% | 1.85% | |
| – major | 3.16% | 4.69% | |
| Stack scan | 0.52% | 0.34% | |

| Time split | `wbu` (106 s) | `wbu_iter` (71 s) |
|---|---:|---:|
| RHS evaluation | 41.4%, ~44 s | 63.2%, ~45 s |
| Global update, total | 35.5%, ~38 s | 4.1%, ~3 s |
| – lookups | 33.3%, ~35 s | 1.7%, ~1 s |
| Local update | 11.7%, ~12 s | 16.6%, ~12 s |
| Post-solve check | 6.3%, ~7 s | 10.2%, ~7 s |
| Global read | 3.1%, ~3 s | 4.0%, ~3 s |
| Everything else | 2.0%, ~2 s | 1.9%, ~1 s |

### `pingfs`: RHS 168808, TF 168551, `SV-COMP result: unknown`

| | `wbu` | `wbu_iter` | Change |
|---|---:|---:|---:|
| Solver time | 82.3 s | 49.0 s | −40.5% |
| Wall time | 99.8 s | 67.9 s | −31.9% |
| Instructions | 566.9 G | 370.9 G | −34.6% |
| Peak RSS | 538.7 MB | 384.0 MB | −28.7% |
| Top heap words | 69.2 M | 45.5 M | −34.3% |
| Minor words | 39.2 G | 27.7 G | −29.4% |
| Promoted words | 286.6 M | 269.2 M | −6.1% |
| Minor collections | 149729 | 105756 | −29.4% |
| Major collections | 49 | 61 | +24.5% |
| GC share | 6.50% | 9.16% | |
| – minor | 2.16% | 2.76% | |
| – major | 4.34% | 6.40% | |
| Stack scan | 0.51% | 0.46% | |

| Time split | `wbu` | `wbu_iter` |
|---|---:|---:|
| RHS evaluation | 40.4% | 58.8% |
| Post-solve check | 17.5% | 26.4% |
| Global update, total | 31.2% | 3.9% |
| – lookups | 27.7% | 0.5% |
| Local update | 5.2% | 6.8% |
| Global read | 3.8% | 2.1% |
| Everything else | 2.0% | 2.0% |

### `uthash`: RHS 474146, TF 637123, `SV-COMP result: true`

The `wbu` run is from an earlier batch than the `wbu_iter` run at `cff0bb08e`.

| | `wbu` | `wbu_iter` | Change |
|---|---:|---:|---:|
| Solver time | 467.8 s | 172.2 s | −63.2% |
| Wall time | 492.8 s | 201.0 s | −59.2% |
| Instructions | 2053 G | 1014 G | −50.6% |
| Peak RSS | 1387 MB | 444 MB | −68.0% |
| Top heap words | 179.0 M | 50.9 M | −71.6% |
| Minor words | 176.9 G | 76.0 G | −57.0% |
| Promoted words | 1001 M | 716 M | −28.5% |
| Major collections | 85 | 144 | +69.4% |
| GC share | 6.49% | 8.69% | |
| – minor | 2.78% | 2.42% | |
| – major | 3.71% | 6.27% | |
| Stack scan | 1.43% | 0.49% | |

Time split for `wbu_iter` at 201 s: RHS 62.1%, ~125 s. Post-solve 16.8%, ~34 s. Local update
9.8%, ~20 s. Global update 5.7%, ~11 s. Global read 3.7%, ~7 s. The rest 2%. For `wbu` at
493 s: RHS ~155 s, global update 46%, ~227 s, of which lookups are ~177 s.

Steps on `uthash`:

| Version | Solver time | Wall time | Instructions | Peak RSS | Minor words | Promoted words | Major collections |
|---|---:|---:|---:|---:|---:|---:|---:|
| `wbu` | 467.8 s | 492.8 s | 2053 G | 1387 MB | 176.9 G | 1001 M | 85 |
| `7ee1fa44f` | 288.9 s | 323.1 s | 1311 G | 408 MB | 103.1 G | 829 M | 173 |
| `b8180dff5` | 206.2 s | 231.4 s | 1205 G | 408 MB | 95.4 G | 792 M | 163 |
| `cff0bb08e` | 172.2 s | 201.0 s | 1014 G | 444 MB | 76.0 G | 716 M | 144 |

The last two rows ran side by side, and the first two ran side by side in an earlier batch.

## Program scouting

All with `wbu_iter` on the same config, run in parallel on a loaded machine, so times are
rough. `vhost` passed the 600 s limit during an earlier scouting round, but took 234 s in its
comparison, so a program killed here may still fit.

**Finished in the latest round, with a 600 s limit:**

| Program | Wall time | Peak RSS | RHS | Result |
|---|---:|---:|---:|---|
| `ttusb_dec` | 60 s | 135 MB | 33061 | `true` |
| `whiteheat` | 152 s | 488 MB | 184437 | `true` |
| `it87` | 493 s | 346 MB | 124524 | `true` |

**Killed at 600 s in the latest round, with no RHS count or result:**

| Program | Peak RSS when killed |
|---|---:|
| `eni` | 645 MB |
| `dc395x` | 744 MB |
| `btmrvl` | 766 MB |
| `msp3400` | 809 MB |
| `farsync` | 840 MB |
| `ppp_generic` | 986 MB |
| `et131x` | 1418 MB |
| `sis900` | 1646 MB |
| `pktcdvd` | 1895 MB |

The round ran these 12 programs on 12 cores, from `sv-benchmarks/c/ldv-linux-3.0`.

**Earlier rounds:**

- Hit the 15-minute limit: `libaco`, `easylogger`, `nnn-mini`, `cut-1`, `atl1c`, `synclink`,
  `megaraid`.
- Hit the 10-minute limit: `magicmouse`, `sleep-1`, `basename-1`, `echo-1`, `tee-1`, `head-1`,
  `gigaset`.
- Too small: `easylogger-mini`, `kbtab`, and `snoopy` at about 30 s in the artifact's results.
- Frontend error: `procdump-mini`.

## Findings

- Results are identical on all four programs.
- The saving comes mostly from global update lookups. RHS, post-solve and local update cost
  about the same in seconds on `ipmi` and `vhost`.
- The gain varies a lot, and `uthash` is not the best case. An earlier claim that it was
  turned out to be wrong.
- Speed and memory gains do not move together: `vhost` has −90.8% solver time but −19.1% RSS.
- Stack scanning is at most 1.43%, so stack depth was not the main cost. An early hypothesis
  that it was turned out to be wrong.
- Older artifact results on `uthash`, not rerun: TD 41787 RHS, `wbu` without digests 17326 RHS,
  against 474146 here.

## Open

- Next comparison: `it87`, the closest fit from scouting. A run was started and then stopped, so it has no results.
- RHS count work.
- A second join shortcut for growing contributions, the order of global todos, dropping them
  on abort, and coalescing evaluations.
- `domain_list ()` rebuilt per digest comparison in `mCPRegistry.ml`, outside the solver.
