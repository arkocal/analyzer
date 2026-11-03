# SV-Comp with the parallel solvers: TODO

Starting point: `conf/svcomp26/parallel.json` on top of svcomp26 common + verify + a level
(td_parallel_immediate, 4 domains, saturn hashmap, hashcons on via GobHashcons, arg off).
td_parallel_independent is dropped for now: it can return a sound solution that is not a fixpoint (verify error,
seen on pthread-wmm/mix007.oepc with level01-nooct).
Benchmarks: `~/Workspace/sv-easy` (tools `l0-*`, `l1noct-*`, `l1pre-*`; limits `quick-par4`/`full-par4`, `q60-par4`/`q60-p4`).
All commits named below are on branch `svcomp-parallel-wip`.

## Current state (2026-11-03)
- **Soundness:** immediate is sound on all of conc-all at level00 and level01-nooct (congruence on), no wrong verdicts and
  no verify errors (benchmark 2026-10-30, see `svcomp_bench_2026-10-30.md`). Sanity runs on memsafety/overflow and
  level04-nooct after the 11-02 fixes agree with td3.
- **Performance is the weak point:** only ~1.7–2 of 4 domains busy; walltime 1.1–1.6× faster, but CPU 1.2–1.6× higher
  (up to 2× on the largest task). SV-Comp limits summed CPU time, so at equal CPU immediate does less work than td3.
  80% of conc tasks take td3 < 0.5s, where parallelism can only add fixed overhead.
- **Parallel portfolio candidate:** level00, level01-nooct, level04-nooct. Everything with Apron (octagon, level02–05,
  and termination at every level) stays on td3 until Apron is made domain-safe.
- **Not yet upstreamable:** depends on a local CIL commit and a TEMP OCaml 5.4.1 switch (39ae1d1b5); four commits need
  Ali's review (see below).

## Next, in order
1. Ali: review 46bc2176b, 8b99acf5e, cde77826f, 76dff51d8 (checklists below).
2. Push the CIL fix (local commit 4dde7331, `~/Workspace/cil` branch `domain-local-alignment`), goblint/cil PR,
   bump the pin in `goblint.opam`/`goblint.opam.locked`, `opam pin remove goblint-cil` locally.
3. Profile the fixed overhead of immediate on a tiny task (domain startup, Saturn table, sharded hashcons).
4. Measure td3 sequential cost of GobHashcons (`l0-td3` vs `l0-td3-nohc`).
5. Big benchmark of level00 / level01-nooct / level04-nooct on conc-all + memsafety + overflow (only when asked; blocks
   the machine). Needs the BenchExec hang fixed or worked around first (watchdog restarting on a stalled log).
6. Apron (see below).
7. Portfolio design.

## Must fix before a parallel config is used for real
- [x] **RichVarinfo / heap vars (unsound).** `BiVarinfoMap` (mallocWrapper's `NodeVarinfoMap`) writes `xh` then `vh`,
      both plain Hashtbls. `IsHeapVar`/`IsMultiple` read `vh`, so a lookup in between or an entry lost in a resize says
      "not heap / unique" → strong updates. Fixed in 46bc2176b (Saturn.Htbl, insert into `vh` before publishing in `xh`).
  - [ ] **Ali: understand and review 46bc2176b** (`src/common/util/richVarinfo.ml`). Check in particular: the
        `vh`-before-`xh` ordering argument, racing creations returning the first published varinfo, the second
        `vh` insert being harmless (vids come from name+type), and the changed marshal format (incremental).
- [ ] **Integrate the CIL fix properly.** `ignoreAlignmentAttrs` → `Domain.DLS`, plus `domain_shims` dependency.
      Now local commit 4dde7331 in `~/Workspace/cil` (branch `domain-local-alignment`, path-pinned into the analyzer
      switch; the original uncommitted version was lost on the other machine). Ali pushes it. See "Next" step 2.
      Without it: `Assertion failed` at cil.ml:2368, or silently ignored `aligned` attributes (wrong sizes/offsets).
- [ ] **Other CIL globals:** ~35 top-level refs in cil.ml; check which are reachable from transfer functions.

## Decide
- [x] **Hashcons.** BatHashcons' tag counter is non-atomic (unsound). No usable parallel hashcons library exists
      (ocaml-hashcons: not thread-safe; hc 0.5: one global Mutex; Saturn: no weak table). `GobHashcons` in 8b99acf5e:
      chosen by solver, sharded + locked for `td_parallel_*`, single weak table otherwise; parallel.json has hashcons on.
  - [ ] **Ali: understand and review 8b99acf5e.** Check: mode selection via AfterConfig (solver name prefix), the shared
        atomic tag counter, shard choice by hash bits, DomainsafeLazy for the shards, stdlib `Weak.Make` instead of
        BatHashcons' table in sequential mode (sequential performance, cf. the batteries pin #2050), and the spinlock
        instead of GobMutex for the shards (see below).
  - [x] First version locked the shards with `GobMutex`: 17 verify errors + 1 segfault on conc-all (immed, level01-nooct).
        Under domainslib, waiting for a GobMutex (Domain-Local-Await) suspends the task and runs other tasks on the same
        domain (or resumes it on another), clobbering domain-local state of the suspended transfer function
        (`current_node`, `executing_speculative_computations`, Timing stack). mix018_tso.oepc: GobMutex 3/100 fixpoint
        errors, Stdlib.Mutex 0/100, spinlock 0/100. Amended into 8b99acf5e.
  - [x] Rerun conc-all with immed + hashcons on: done 2026-10-30, sound (0 wrong, 0 verify errors).
  - [ ] Measure td3 sequential cost of the new table (`l0-td3` vs `l0-td3-nohc`).
- [x] Refuse `exp.arg.enabled` together with a parallel solver (57272bf71).
- [x] Validation portfolio stays sequential; precision loss from ignored `solvers.td3.*` options (widen_gas,
      side_widen_gas, narrow-globs) accepted (decided 2026-11-02).

## Robustness (crash/hang, not unsound)
- [x] Plain `Lazy` forced from worker domains → `DomainsafeLazy`: `Offset.Index.Exp.any`/`all`
      (`Cilfacade.any_index_exp`), base `array_domain_annotation_enabled`, `LibraryFunctions.intmax_t`, `RichVarinfo.single`.
      Actually crashed immediate on fillarray1 (level01-pre). Fixed in cde77826f.
  - [ ] **Ali: understand and review cde77826f.** Check: the selection (module-level lazies forced in transfer
        functions; call-local lazies such as in longjmpLifter/access/apronDomain left alone; spec-module lazies in
        base/basePriv/apron are forced before solving), `Offset_intf` signature change, `goblint_parallel` dependency
        of `goblint_common`.
- [x] `DomainsafeLazy.force` waited via Domain-Local-Await (same task-suspension problem as GobMutex above). Now spins:
      95fc586b9. The only other GobMutex use in transfer functions is `Goblint_tracing.trace_mutex` (tracing builds only, left as is).
- [x] `Base.char_array` → Saturn.Htbl with immutable strings + CAS (856eb8e23); `Base.array_map` → Saturn.Htbl (463ecf342,
      marshal format changed).
- [x] Refuse `exp.arg.enabled`, `warn_at early` and `gobview` with `td_parallel_*` (57272bf71): with warnings during solving,
      Messages/Checks tables are written from several domains. With `warn_at post` all of these are gated by `should_warn`/`postsolving`.
- Accepted (decided 2026-11-02): races that only cause repeated work: GobConfig getter cache, PrecisionUtil/IntDomain0/FloatDomain
  config caches, ResettableLazy tables (WideningThresholds, Cilfacade stmt/varinfo tables, `activated_library_descs`). `ThreadId.tids` is dbg only.
- [ ] `OwnConcurrentHashmap` resize race (see `concurrent_hashmap_resize_bug.md`); parallel.json uses saturn meanwhile.
- [ ] BenchExec hangs reproducibly on conc-all (10-28 and 10-30, no goblint process left), so l0-td3 of 10-30 is
      incomplete (1151/3274 runs). Watchdog idea: restart when the log stops changing.

## Environment (this machine)
- [x] gcc-multilib installed; goblint-cil rebuilt so the 32-bit machdep exists. Before that, every sv-easy run analyzed
      32-bit tasks with host (64-bit) sizes ("Machine definition not available…"); results up to 09:40 on 2026-10-28 are affected.
- [ ] TEMP commit 39ae1d1b5 switches to OCaml 5.4.1 (with domainslib); must be resolved before upstreaming.

## Levels
- [x] `conf/svcomp26/level01-pre.json`: level01 without autotune `congruence` and `octagon` (sv-easy tools `l1pre-*`).
- [x] Domain-local `current_node`: ported from Felix's 6fbc59004 in 76dff51d8.
  - [ ] **Ali: understand and review 76dff51d8.** Check: all readers covered (precisionUtil, messages, checks; the
        base/raceAnalysis/extractPthread readers from Felix's commit are gone), the save/restore in `Constraints.tf`,
        that `Messages.current_context` (same pattern, not changed) is only cosmetic.
  - [x] Re-enabled autotune `congruence` (level01-nooct) and benchmarked: sound on conc-all (2026-10-30).
- [x] `conf/svcomp26/level04-nooct.json` (ed69275fd).
- [x] Audit (2026-11-02) of all analyses in level00/01/04 plus the autotune/autosound extras (memsafety: useAfterFree,
      memOutOfBounds, memLeak; longjmp analyses; tmpSpecial; pthreadBarriers/Once): no further races during solving
      beyond the fixed ones. svcomp_may_* flags and dead-branch flags are only set when postsolving/should_warn.
- Sanity runs (td3 vs immed, data race / memsafety / overflow) all agree. Relative runtime outliers (not soundness):
  list-ext-properties/list-ext_2 (memsafety, level01-nooct): immed 1.6× wall, 3.2× CPU of td3;
  list-ext-properties/960521-1_1-1 (memsafety, level04-nooct): immed 2× wall, 4× CPU.
  level04 times out (120s) on 28-race_reach_05/73 and spi-tegra with both solvers.

## Apron (last, decided 2026-11-02)
- [ ] `octagon`, all of level02–05, and **termination at every level** (autotune `termination` activates apron
      polyhedra, autoTune.ml:248) stay on td3 until this is done. C side not domain-safe: non-atomic env/manager
      refcounts, freed by OCaml finalizers on any domain; manager `internal` scratch space is per thread. Needs a local
      Apron patch (atomic refcounts), per-domain managers (global `Oct.manager_alloc ()`, ~64 `Man.mgr` uses),
      `VarMetadataTbl` → Saturn.Htbl, audit of relationAnalysis state. Hashcons (#1956) is solved by GobHashcons.

## Later
- [ ] td3 does not terminate on `goblint-regression/04-mutex_05-lockfuns` (no-overflow, level00; unknowns keep growing),
      the parallel solvers finish in 0.1s. Still seen on 10-30 (32-bit machdep fixed). Probably side-widening/context difference.
- [ ] Portfolio design. Keep in mind: SV-Comp limits CPU time summed over cores (see the CPU overhead above).
- [ ] svcomp27 confs don't exist yet; sv-easy's `sv.yml` now differs from the laptop copy.
