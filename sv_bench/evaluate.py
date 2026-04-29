import marimo

__generated_with = "0.23.3"
app = marimo.App()


@app.cell
def _():
    import pandas as pd
    import matplotlib.pyplot as plt
    import marimo as mo
    import numpy as np

    return mo, np, pd, plt


@app.cell
def _(pd):
    df_raw = pd.read_csv('results/combined.csv')
    return (df_raw,)


@app.cell
def _(df_raw):
    all_sources = sorted({
        src.strip()
        for sources in df_raw['sources'].dropna()
        for src in sources.split('|')
    })
    return (all_sources,)


@app.cell
def _(all_sources, mo):
    source_selector = mo.ui.dropdown(all_sources, label="Filter by source")
    source_selector
    return (source_selector,)


@app.cell
def _(mo):
    min_runtime_filter = mo.ui.number(value=0, label="Min runtime (s) — task passes if any config takes at least this long")
    min_runtime_filter
    return (min_runtime_filter,)


@app.cell
def _(df_raw, min_runtime_filter, source_selector):
    def classify(row):
        ret = str(row['returned']).lower()
        exp = str(row['expected']).lower()
        if ret in ('true', 'false') and ret == exp:
            return 'right'
        elif ret in ('true', 'false'):
            return 'wrong'
        else:
            return 'unknown'

    df = df_raw.copy()
    if source_selector.value:
        df = df[df['sources'].fillna('').apply(
            lambda s: source_selector.value in [x.strip() for x in s.split('|')]
        )]
    if min_runtime_filter.value:
        passing = df.groupby(['task', 'property'])['runtime'].max()
        passing = passing[passing >= min_runtime_filter.value].index
        df = df.set_index(['task', 'property']).loc[passing].reset_index()
    df['verdict'] = df.apply(classify, axis=1)
    df['time_per_rhs'] = df['solver_walltime'] / df['rhs_evals']
    return (df,)


# @app.cell
# def _(df, mo):
#     mo.ui.table(df)
#     return


@app.cell
def _(df):
    summary = (
        df.groupby(['config', 'verdict'])
          .size()
          .unstack(fill_value=0)
          .reindex(columns=['right', 'wrong', 'unknown'], fill_value=0)
    )
    timeouts = df[df['timeout'] == True].groupby('config').size().rename('timeout')
    summary = summary.join(timeouts, how='left').fillna(0).astype(int)
    return (summary,)


@app.cell
def _(summary):
    summary
    return


@app.cell
def _(mo, plt):
    def cactus_plot(df, metric, ylabel):
        fig, ax = plt.subplots(figsize=(8, 5))
        for cfg, group in df.groupby('config'):
            right = group[group['verdict'] == 'right'].dropna(subset=[metric]).sort_values(metric)
            n_solved = list(range(len(right) + 1))
            metric_values = [0] + list(right[metric])
            ax.plot(n_solved, metric_values, marker='o', markersize=4, linewidth=1, label=cfg)
        ax.set_xlabel('Tasks solved correctly')
        ax.set_ylabel(ylabel)
        ax.set_title('Cactus plot')
        ax.legend()
        plt.tight_layout()
        return mo.mpl.interactive(fig)

    return (cactus_plot,)


@app.cell
def _(cactus_plot, df):
    cactus_plot(df, "runtime", "runtime")
    return


@app.cell
def _(np, pd):
    def compare_solvers(df, cfg_a, cfg_b):
        """
        min_filter: optional (metric, value) tuple — keeps only tasks where
                    at least one solver has metric >= value.
        Returns (table DataFrame, stats dict).
        """
        key = ['task', 'property']

        def cols(frame):
            return frame[['runtime', 'rhs_evals', 'solver_walltime']]

        def format_float(s, digits=2):
            return f"{s:.{digits}f}"

        def drop_common_timeouts(af, bf):
            common_to = (
                af[af['timeout'] == True].index
                .intersection(bf[bf['timeout'] == True].index)
            )
            return af[~af.index.isin(common_to)], bf[~bf.index.isin(common_to)], len(common_to)

        def drop_any_timeout(af, bf):
            a_to = af[af['timeout'] == True].index
            b_to = bf[bf['timeout'] == True].index
            any_to = a_to.union(b_to)
            return (
                af[~af.index.isin(any_to)],
                bf[~bf.index.isin(any_to)],
                len(a_to.difference(b_to)),  # only a timed out
                len(b_to.difference(a_to)),  # only b timed out
            )

        a_full_raw = df[df['config'] == cfg_a].set_index(key)
        b_full_raw = df[df['config'] == cfg_b].set_index(key)

        a_full, b_full, n_common_timeouts = drop_common_timeouts(a_full_raw, b_full_raw)
        _, _, n_only_a_timeout, n_only_b_timeout = drop_any_timeout(a_full, b_full)

        a_full = a_full.copy()
        b_full = b_full.copy()
        a_full['speedup_over_other'] = b_full['solver_walltime'] / a_full['solver_walltime']
        b_full['speedup_over_other'] = a_full['solver_walltime'] / b_full['solver_walltime']
        a_full['relative_rhs'] = a_full['rhs_evals'] / b_full['rhs_evals']
        b_full['relative_rhs'] = b_full['rhs_evals'] / a_full['rhs_evals']

        a_right = a_full[a_full['verdict'] == 'right']
        b_right = b_full[b_full['verdict'] == 'right']

        a_unknown = a_full[a_full['verdict'] == 'unknown']
        b_unknown = b_full[b_full['verdict'] == 'unknown']

        a_timeout = a_full[a_full['timeout']]
        b_timeout = b_full[a_full['timeout']]

        a_right_where_b_timeout_idx = a_right.index.intersection(b_timeout.index)
        a_right_where_b_unknown_idx = a_right.index.intersection(b_unknown.index)
        b_right_where_a_timeout_idx = b_right.index.intersection(a_timeout.index)
        b_right_where_a_unknown_idx = b_right.index.intersection(a_unknown.index)

        a_right_where_b_right_idx = a_right.index.intersection(b_right.index)
        b_right_where_a_right_idx = b_right.index.intersection(a_right.index)

        a_unknown_where_b_unknown_idx = a_unknown.index.intersection(b_unknown.index)
        b_unknown_where_a_unknown_idx = b_unknown.index.intersection(a_unknown.index)

        stats = {
            "timeout — both":            n_common_timeouts,
            f"{cfg_a} right, {cfg_b} timeout": len(a_right_where_b_timeout_idx),
            f"{cfg_a} right, {cfg_b} unknown": len(a_right_where_b_unknown_idx),
            f"{cfg_b} right, {cfg_a} timeout": len(b_right_where_a_timeout_idx),
            f"{cfg_b} right, {cfg_a} unknown": len(b_right_where_a_unknown_idx),
        }

        # From now on, we are only looking at cases where no solver timed out

        # Do three tables, each has columns configs and rows:
        # average runtime, median runtime, average rhs_evals, median rhs_eval

        # First for solved by both, second for unknown by both, third for everything.

        df_a_both_right = a_full.loc[a_right_where_b_right_idx]
        df_b_both_right = b_full.loc[b_right_where_a_right_idx]

        def make_results_table(df_a, df_b):
            def make_column(df):
                return pd.Series({
                    # These are the more important metrics
                    # (Geometric mean is more appropriate for speedups, but median is more intuitive, so we show both)
                    # Also, average time is not as meaningful, as the larger tasks dominate it
                    'geomean speedup': format_float(np.exp(np.log(df['speedup_over_other']).mean())),
                    'geomean relative_rhs': format_float(np.exp(np.log(df['relative_rhs']).mean())),
                    'med speedup': format_float(df['speedup_over_other'].median()),
                    'med relative_rhs': format_float(df['relative_rhs'].median()),
                    # 'avg speedup': format_float(df['speedup_over_other'].mean()),
                    # 'avg relative_rhs': format_float(df['relative_rhs'].mean()),

                    # Solver walltime is more important, so lets hide these
                    # 'avg runtime': format_float(df['runtime'].mean()),
                    # 'med runtime': format_float(df['runtime'].median()),

                    # Avg is dominated by large tasks, so we show geomeans of speedups.
                    # 'avg solver walltime': format_float(df['solver_walltime'].mean()),
                    # 'med solver walltime': format_float(df['solver_walltime'].median()),
                    # 'avg rhs_evals': format_float(df['rhs_evals'].mean()),
                    # 'med rhs_evals': format_float(df['rhs_evals'].median()),
                })
            return pd.DataFrame({
                cfg_a: make_column(df_a),
                cfg_b: make_column(df_b),
            })

        # Both unknown, but filter out timeouts
        both_unknown_without_timeouts_idx = a_unknown_where_b_unknown_idx.difference(a_timeout.index.union(b_timeout.index))
        df_a_both_unknown = a_full.loc[both_unknown_without_timeouts_idx]
        df_b_both_unknown = b_full.loc[both_unknown_without_timeouts_idx]

        # Both same and no timeouts
        df_a_both_same_without_timeouts = a_full.loc[a_right_where_b_right_idx.union(a_unknown_where_b_unknown_idx)]
        df_b_both_same_without_timeouts = b_full.loc[b_right_where_a_right_idx.union(b_unknown_where_a_unknown_idx)]

        tables = [
            (f"Both correct (n={len(a_right_where_b_right_idx)})", make_results_table(df_a_both_right, df_b_both_right)),
            (f"Both unknown, no timeout (n={len(both_unknown_without_timeouts_idx)})", make_results_table(df_a_both_unknown, df_b_both_unknown)),
            (f"Both same verdict, no timeout (n={len(df_a_both_same_without_timeouts)})", make_results_table(df_a_both_same_without_timeouts, df_b_both_same_without_timeouts)),
        ]
        return tables, stats

    return (compare_solvers,)


@app.cell
def _(compare_solvers, df, mo):
    _tables, _stats = compare_solvers(df, "td3.json", "fwd.json")
    mo.vstack([
        mo.hstack([mo.stat(label=k, value=str(v)) for k, v in _stats.items()]),
        *[item for title, table in _tables for item in [mo.md(f"### {title}"), table]],
    ])
    return


if __name__ == "__main__":
    app.run()
