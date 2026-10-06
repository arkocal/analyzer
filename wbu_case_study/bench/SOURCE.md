# Where the inputs and configs come from

- `concrat/`: copied unchanged from the TACAS'26 artifact (`parallel-td-artifact-tacas26/docker/bench/concrat`,
  commit 04979bbbdd55481db353fd1b7cb44da870ff4401), which took them from https://github.com/kaist-plrg/concrat.
- `conf/level01.json`: copied unchanged from the wbu paper's forward artifact (`forward-artifact/goblint-configs`, 2b2dbc51b3570d56e4bcf3b2a833940dd949cd4c).
- `conf/casestudy.json`: level01's `ana.activated` plus `malloc_null`, `memLeak` and `uninit` (all three are already in
  level01's `ana.path_sens`). Written as a full list because a conf file cannot append.
- `no-data-race.prp`: from sv-benchmarks (`c/properties`, ca46fe670b5dcbcd3495bd23689353a8ea1fb86b).
- `../solver-configs/`: copied unchanged from the forward artifact's `goblint-configs`.
