# Reference sources (not built)

Files here are carried over from other branches for reference only. `src/solver/dune`
sets `(include_subdirs no)`, so nothing in this directory is compiled — these files are
not expected to build against the current tree.

## `fwdCommon.ml`

The full `src/solver/fwdCommon.ml` from the `fwd_constrsys_new` branch.

Its head — `WarrowConfig` and the `Warrow` functor, i.e. the abstracted update rules
(widening delay, narrowing gas, update gas) — *is* built, as `src/solver/fwdCommon.ml`,
because `td_simplified_ref_improved.ml` uses it.

The rest is kept only as inspiration: `SolverLocals`, `SolverGlobals`, `Checker` and the
signatures around them show how the forward solvers apply those update rules to locals
and globals separately (a `Warrow` instance per source node, joined into the global
value). It needs `FwdGlobConstrSys`, which does not exist on this branch.
