# Plan: Identify Shared Infrastructure Between control.ml and fwdControl.ml

## Context

`control.ml` and `fwdControl.ml` are two parallel analysis pipelines. ~60–70% of the code is identical. The goal here is to catalog what can be extracted into a shared module *without* the LVar type unification work, as the first step toward reducing duplication before adding SV-COMP support to the backwards solvers.

## Key Constraint: Why Sharing Is Limited

`BaseGlobConstrSys` (the common ancestor of both `FwdGlobConstrSys` and `DemandGlobConstrSys`) only exposes `LVar`, `GVar`, `D`, `G` — it has **no `G.spec`, `GVar.spec`, or `G.create_spec`**. These accessors exist on the concrete types (`GVarF`, `GVarFCNW`, `GVar3`, `GVarG`) but are absent from any shared signature. Most of the "identical-looking" functions actually thread through these accessors, which blocks direct extraction.

## Catalog

### Group A — Trivially shared, zero changes

| Code | Lines | Notes |
|---|---|---|
| `analyze_loop` / `analyze` | ~15 | Identical in both files, no type dependency |
| `current_node_state_json` / `current_varquery_global_state_json` refs | ~2 | Identical declarations |

### Group B — Shared with one small parameter

| Code | Difference | Fix |
|---|---|---|
| `spec_module` / `get_spec` | fwdControl adds `NoDigestLifter` and tweaks `PathSensitive2` condition | One implementation with `~fwd:bool` flag |
| Witness init block (`Witness.init`; `YamlWitness.init`) | Identical | Trivially extracted alongside `spec_module` |

### Group C — Shared with a thin `CommonSpecSys` interface (~150 lines)

These functions are identical but all use `EQSys.G.spec`, `EQSys.GVar.spec`, `EQSys.G.create_spec`. These accessors exist on both concrete system types but are not in `BaseGlobConstrSys`. A small `CommonSpecSys` interface that threads them through would unlock sharing all of:

| Code | Lines (~) |
|---|---|
| `do_extern_inits` | 20 |
| `do_global_inits` | 55 |
| `enter_with` | 15 |
| `otherstate` | 15 |
| `man` builder | 15 |
| `print_globals` / `make_global_fast_xml` | 12 |
| `startvars`/`exitvars`/`othervars` construction | 15 |

The interface would add three accessors to `BaseGlobConstrSys`-based types:
```ocaml
module type CommonSpecSys = sig
  module Spec: Spec'
  module EQSys: BaseGlobConstrSys with module D = Spec.D
  val g_spec: EQSys.G.t -> Spec.G.t
  val gvar_spec: Spec.V.t -> EQSys.GVar.t
  val g_create_spec: Spec.G.t -> EQSys.G.t
  module LHT: BatHashtbl.S with type key = EQSys.LVar.t
  module GHT: BatHashtbl.S with type key = EQSys.GVar.t
end
```

Both `SpecSys` and `FwdSpecSys` trivially satisfy this (their concrete `GVar.spec` and `G.spec` are already `\`Left x` in both cases).

### Group D — Blocked until LVar type unification

| Code | Blocker |
|---|---|
| `solver2source_result` | `x.node` (record) vs `(n, es)` (tuple) |
| `print_dead_code` | LT tuple shape differs (4-tuple with digests vs 3-tuple) |
| `RT` / `LT` / `Result` modules | `ResultType2Digest` vs `ResultType2` |
| `warn_global` | `GVarFCNW` has 3 variants (`Left/Middle/Right`); `GVarF` has 2 |
| `solve_and_postprocess` | Entirely different solver machinery |
| SV-COMP / YAML witness blocks | Require `ResultQuery.SpecSysSol2`, tied to LVar type |

## Recommended Extraction Order

1. **Group A** — Extract `analyze_loop`/`analyze` into a shared helper. Zero risk.
2. **Group B** — Unify `spec_module`/`get_spec` with a `~fwd` flag.
3. **Group C** — Add `CommonSpecSys` to `analyses.ml` and extract the ~150 lines of initialization logic. This is the highest-value step and a prerequisite for any further unification.
4. **Group D** — Tackle as a separate effort after LVar type unification.

## Files to Modify

| File | Change |
|---|---|
| `src/framework/analyses.ml` | Add `CommonSpecSys` module type |
| `src/framework/control.ml` | Replace duplicated sections with shared implementations |
| `src/framework/fwdControl.ml` | Same |
| `src/framework/commonControl.ml` (new) | Home for `analyze_loop`, `spec_module`, and Group C helpers |

## Verification

```bash
make
bash fwdregtest.sh fwd
bash fwdregtest.sh wbu
```
