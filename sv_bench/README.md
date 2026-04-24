# sv_bench

Benchmark harness for running goblint on SV-COMP tasks.

## Setup

```
uv sync
```

## Configure

Edit `bench.yaml` to set the goblint binary, configs, tasks, and timeout.

## Run

```
uv run snakemake --cores 8
```

This produces `results/combined.csv`. Jobs are skipped on re-run if already complete.

## Evaluate

Run the snakemake step first, then open the notebook:

```
uv run jupyter notebook evaluate.ipynb
```
