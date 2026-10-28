# SV-Comp with the parallel solvers: TODO

Starting point: `conf/svcomp26/parallel.json` on top of svcomp26 common + verify + level00
(td_parallel_immediate, 4 domains, saturn hashmap, hashcons off, arg off).
td_parallel_independent is dropped for now: it can return a sound solution that is not a fixpoint (verify error,
seen on pthread-wmm/mix007.oepc with level01-nooct).
Benchmarks: `~/Workspace/sv-easy` (tools `l0-td3`, `l0-td3-nohc`, `l0-indep`, `l0-immed`; limits `quick-par4`/`full-par4`).

## Must fix before a parallel config is used for real
- [x] **RichVarinfo / heap vars (unsound).** `BiVarinfoMap` (mallocWrapper's `NodeVarinfoMap`) writes `xh` then `vh`,
      both plain Hashtbls. `IsHeapVar`/`IsMultiple` read `vh`, so a lookup in between or an entry lost in a resize says
      "not heap / unique" → strong updates. Fixed by Claude in 46bc2176b on branch `svcomp-parallel`
      (Saturn.Htbl, insert into `vh` before publishing in `xh`).
  - [ ] **Ali: understand and review 46bc2176b** (`src/common/util/richVarinfo.ml`). Check in particular: the
        `vh`-before-`xh` ordering argument, racing creations returning the first published varinfo, the second
        `vh` insert being harmless (vids come from name+type), and the changed marshal format (incremental).
- [ ] **Integrate the CIL fix properly.** Currently a local, uncommitted change in `~/Workspace/cil`
      (branch `domain-local-alignment`, path-pinned into the analyzer switch): `ignoreAlignmentAttrs` → `Domain.DLS`,
      plus `domain_shims` dependency. To do: goblint/cil PR, bump the pin in `goblint.opam`/`goblint.opam.locked`,
      `opam pin remove goblint-cil` locally. Without it: `Assertion failed` at cil.ml:2368, or silently ignored
      `aligned` attributes (wrong sizes/offsets).
- [ ] **Other CIL globals:** ~35 top-level refs in cil.ml; check which are reachable from transfer functions.

## Decide
- [x] **Hashcons.** BatHashcons' tag counter is non-atomic (unsound). No usable parallel hashcons library exists
      (ocaml-hashcons: not thread-safe; hc 0.5: one global Mutex; Saturn: no weak table). Claude added `GobHashcons`
      in 8100548c1 (branch `svcomp-parallel`): chosen by solver, sharded + locked for `td_parallel_*`, single weak table
      otherwise; parallel.json now has hashcons on.
  - [ ] **Ali: understand and review 8100548c1.** Check: mode selection via AfterConfig (solver name prefix), the shared
        atomic tag counter, shard choice by hash bits, DomainsafeLazy for the shards, stdlib `Weak.Make` instead of
        BatHashcons' table in sequential mode (sequential performance, cf. the batteries pin #2050), and the spinlock
        instead of GobMutex for the shards (see below).
  - [x] First version locked the shards with `GobMutex`: 17 verify errors + 1 segfault on conc-all (immed, level01-nooct).
        Under domainslib, waiting for a GobMutex (Domain-Local-Await) suspends the task and runs other tasks on the same
        domain (or resumes it on another), clobbering domain-local state of the suspended transfer function
        (`current_node`, `executing_speculative_computations`, Timing stack). mix018_tso.oepc: GobMutex 3/100 fixpoint
        errors, Stdlib.Mutex 0/100, spinlock 0/100. Amended into 8100548c1.
  - [ ] Rerun conc-all with immed + hashcons on; measure td3 sequential cost of the new table.
- [ ] Refuse `exp.arg.enabled` together with a parallel solver (ARG forces hashconsing back on).

## Robustness (crash/hang, not unsound)
- [x] Plain `Lazy` forced from worker domains → `DomainsafeLazy`: `Offset.Index.Exp.any`/`all`
      (`Cilfacade.any_index_exp`), base `array_domain_annotation_enabled`, `LibraryFunctions.intmax_t`, `RichVarinfo.single`.
      Actually crashed immediate on fillarray1 (level01-pre). Done by Claude in cde77826f (branch `svcomp-parallel`).
  - [ ] **Ali: understand and review cde77826f.** Check: the selection (module-level lazies forced in transfer
        functions; call-local lazies such as in longjmpLifter/access/apronDomain left alone; spec-module lazies in
        base/basePriv/apron are forced before solving), `Offset_intf` signature change, `goblint_parallel` dependency
        of `goblint_common`.
- [ ] `DomainsafeLazy.force` waits via Domain-Local-Await too (same task-suspension problem as GobMutex above), when
      another domain is forcing the same lazy. Rare (first force only), but should block/spin instead. Same for any other
      GobMutex use inside transfer functions.
- [ ] Shared Hashtbls: GobConfig getter cache (fill before solving), `Base.char_array`, `ThreadId.tids` (dbg only).
- [ ] `OwnConcurrentHashmap` resize race (see `concurrent_hashmap_resize_bug.md`); parallel.json uses saturn meanwhile.

## Environment (this machine)
- [x] gcc-multilib installed; goblint-cil rebuilt so the 32-bit machdep exists. Before that, every sv-easy run analyzed
      32-bit tasks with host (64-bit) sizes ("Machine definition not available…"); results up to 09:40 on 2026-10-28 are affected.

## Level01
- [x] `conf/svcomp26/level01-pre.json`: level01 without autotune `congruence` and `octagon` (sv-easy tools `l1pre-*`).
- [x] Domain-local `current_node`: ported from Felix's 6fbc59004 by Claude in 76dff51d8 (branch `svcomp-parallel`).
  - [ ] **Ali: understand and review 76dff51d8.** Check: all readers covered (precisionUtil, messages, checks; the
        base/raceAnalysis/extractPthread readers from Felix's commit are gone), the save/restore in `Constraints.tf`,
        that `Messages.current_context` (same pattern, not changed) is only cosmetic.
  - [ ] Then re-enable autotune `congruence` (level01 minus octagon) and benchmark.
- [ ] Apron (`octagon`, and all of level02–05): **kept out of parallel configs for now** (decided 2026-10-28; portfolio
      would run Apron levels with td3). C side not domain-safe: non-atomic env/manager refcounts, freed by OCaml
      finalizers on any domain; manager `internal` scratch space is per thread. Needs a local Apron patch (atomic
      refcounts), per-domain managers (global `Oct.manager_alloc ()`, ~64 `Man.mgr` uses), `VarMetadataTbl` →
      Saturn.Htbl, audit of relationAnalysis state. Hashcons (#1956) is solved by GobHashcons.

## Later
- [ ] td3 does not terminate on `goblint-regression/04-mutex_05-lockfuns` (no-overflow, level00; unknowns keep growing),
      the parallel solvers finish in 0.1s. Seen with 64-bit machdep; recheck. Probably side-widening/context difference.
- [ ] **`current_node` / `Messages.current_context` race.** Matters once autotune `congruence` (annotation.int) is on,
      i.e. for levels above level00. Felix had a DLS `current_node` on `arkocal/td_parallel_new` (6fbc59004).
- [ ] Go beyond level00: audit the analyses of level01–05 (apron etc.) for global state.
- [ ] Portfolio design. Keep in mind: SV-Comp limits CPU time summed over cores.
- [ ] svcomp27 confs don't exist yet; sv-easy's `sv.yml` now differs from the laptop copy.
