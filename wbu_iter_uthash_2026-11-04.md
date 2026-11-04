**On `uthash`, `wbu_iter` uses 71% less memory and about 36% fewer instructions than `wbu`, with identical results, and no new blocker showed up.** The runtime hit your target: about 5.4 minutes for `wbu_iter` and 8.2 for `wbu`. This was one run per solver, side by side, using `level01` + `casestudy` + `wbu_digest_gc_abort_set`.

| | `wbu` | `wbu_iter` | Change |
|---|---|---|---|
| Solver time | 467.8 s | 288.9 s | **−38.2%** |
| Wall time | 492.8 s | 323.1 s | −34.4% |
| Instructions | 2053 G | 1311 G | **−36.1%** |
| Peak RSS | 1387 MB | 408 MB | **−70.6%** |
| Top heap words | 179.0 M | 50.9 M | −71.6% |
| Minor heap words allocated | 176.9 G | 103.1 G | −41.7% |
| Promoted words | 1001 M | 829 M | −17.2% |
| Minor collections | 674883 | 393566 | −41.7% |
| Major collections | 85 | 173 | about double |

**Correctness:** RHS (474146) and TF (637123) match, the output diff is empty, both report `SV-COMP result: true`, and there are no fixpoint errors.

| Share of total runtime | `wbu` | `wbu_iter` |
|---|---|---|
| **GC total** | **6.5%** | **7.9%** |
| – minor collections | 2.9% | 2.6% |
| – major collections | 3.7% | 5.3% |
| Stack scan (`caml_oldify_local_roots`, part of minor) | 1.43% | 0.66% |

- **GC:** its share rises for `wbu_iter` only because the total shrank. GC samples fall by about 20%, from about 16000 to about 12700. The smaller heap is probably why major collections doubled, though they stay cheap.
- **Stack scan:** this is now measurable, at 1.43% for `wbu` against 0.33–0.47% on the small inputs, so it does grow with program size. It's still only about 5 s of a 468 s run. `wbu_iter`'s remaining 0.66% is probably scanning of global roots, but I haven't confirmed that.
- **Memory:** `wbu` keeps about 1 GB more, which fits the `LM.add` leak in the `last` table. I didn't run the versions in between on this program, so I can't split the gain between that fix, the other removed lookups and the recursion removal.
- **Blockers:** the old hot symbols are gone (`analyses.ml:54` fell from 6712 samples to 62, and `Hashtbl.find` from 5676 to 0). What's left at the top is flat and mostly analysis code or general runtime, with no symbol above 5%. I skipped the call-graph runs, since nothing here needed attributing.

So `wbu_iter` looks ready to serve as the base for the RHS count work, which is what's next. RHS count now tells you most of the story on this config: 474146 evaluations in about 289 s. For comparison, the older build's artifact had the TD baseline at 41787 evaluations on `uthash`, and `wbu` without digests at 17326. So there's a large gap to look into, though those two used an older build and I haven't rerun them here.
