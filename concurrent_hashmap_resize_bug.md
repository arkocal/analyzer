# OwnConcurrentHashmap: reads race with resize

Found while reviewing the td_parallel PR (2026-09-29). Upstream code in
`src/util/parallel/data.ml` (unchanged since `61fbf8566`), made reachable by the
parallel solvers: `td_parallel_immediate` uses it by default (`solvers.td_parallel.hashmap = own`).

## Problem

`find_option` reads the bucket array and its size in two separate atomic reads:

```ocaml
let bucket = Array.get (Atomic.get hm.buckets) (hash mod (Atomic.get hm.nr_buckets)) in
```

while `resize` publishes them one after the other:

```ocaml
Atomic.set hm.buckets new_buckets;
Atomic.set hm.nr_buckets new_size;
```

A concurrent resize between the two reads pairs one generation's array with the
other's size (argument evaluation order is unspecified, so both can happen):

- old size `n` + new array `2n`: wrong bucket for about half the keys, so
  `find_option` returns `None` for a present key and `find` raises `Not_found`.
  `td_parallel_immediate` calls `CM.find` in `destab_single` and in the outer
  `solver ()` loop, expecting the key to exist, so the solver crashes.
- old array `n` + new size `2n`: index up to `2n-1`, so `Array.get` raises
  `Invalid_argument "index out of bounds"`.

`find_create_inner` has the same pattern (indexes the passed-in array with a fresh
read of `nr_buckets`); its generation check does not cover the out-of-bounds case.

## Fix

Derive the size from the array actually loaded:

```ocaml
let buckets = Atomic.get hm.buckets in
let bucket = Array.get buckets (hash mod Array.length buckets) in
```

in `find_option`, `find_create_inner` (and `resize` for consistency). `nr_buckets`
is then only needed for the resize threshold.

Alternative/stopgap: make `saturn` the default for `solvers.td_parallel.hashmap`.

## Status (2026-09-29)

- Array/size mismatch: FIXED in `ce05d1107` (`nr_buckets` removed, size taken
  from `Array.length` of the loaded array).
- Still open: stale entries during resize. An insert can append to an old bucket
  after `resize` copied it. The inserter notices (generation changed) and retries
  in the new array, but another domain may already have found the old entry
  (`find_create` accepts found entries via `|| not was_created`; `find_option`
  has no generation check) and keeps using an `Atomic` that is no longer in the
  map. Options: seqlock-style validation (reads retry/wait during a resize), or
  freeze-and-forward cooperative resize (reads never block; bigger rewrite,
  essentially what Saturn.Htbl already does). Decide together with the
  own-vs-Saturn comparison.
