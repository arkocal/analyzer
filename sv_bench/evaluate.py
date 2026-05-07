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
    df_raw
    return (df_raw,)


@app.cell
def _(df_raw, mo):
    _pivot = df_raw.pivot_table(
        index=['task', 'property'],
        columns=['base_config', 'config'],
        values='runtime',
        aggfunc='count'
    )
    _missing = _pivot.isna().sum()
    _missing = _missing[_missing > 0]
    mo.callout(
        mo.md("All configs cover every task × property.") if _missing.empty else mo.md(f"Missing combinations:\n\n{_missing.to_string()}"),
        kind="success" if _missing.empty else "warn"
    )
    return


@app.cell
def _(df_raw):
    all_sources = sorted({
        src.strip()
        for sources in df_raw['sources'].dropna()
        for src in sources.split('|')
    })
    all_base_configs = sorted(df_raw['base_config'].dropna().unique())
    all_configs      = sorted(df_raw['config'].dropna().unique())
    return all_base_configs, all_sources


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Virtual Results

    Portfolio runs can easily be simulated, there is no need to rerun everything.

    For this, the timeout needs to be known. The value set will be checked against
    actual timeouts in the data if any are present.
    """)
    return


@app.cell
def _(mo):
    virtual_results_enabled = mo.ui.checkbox(label="Enable virtual results", value=True)
    portfolio_type = mo.ui.dropdown(["serial", "parallel"], value="serial", label="Portfolio type")
    timeout_input = mo.ui.number(value=600, label="TIMEOUT (s)")
    mo.hstack([virtual_results_enabled, timeout_input, portfolio_type])
    return portfolio_type, timeout_input, virtual_results_enabled


@app.cell
def _(df_raw, mo, timeout_input, virtual_results_enabled):
    TIMEOUT = timeout_input.value

    if not virtual_results_enabled.value:
        warning = None
    else:
        _timed_out = df_raw[df_raw['timeout'] == True]
        if _timed_out.empty:
            warning = mo.callout(mo.md("No timed-out results in CSV; TIMEOUT not verified."), kind="warn")
        else:
            _csv_timeout = int(_timed_out['runtime'].round().mode()[0])
            if _csv_timeout != TIMEOUT:
                warning = mo.callout(mo.md(f"Timeout mismatch: `TIMEOUT = {TIMEOUT}` but CSV implies `{_csv_timeout}`."), kind="danger")
            else:
                warning = None

    warning
    return (TIMEOUT,)


@app.cell
def _(TIMEOUT, pd):
    def make_portfolio(df, columns_same_within_portfolio,
                           columns_merged_within_portfolio, 
                           columns_ignored, portfolio_type="parallel"):
        # These are the same in all rows contributing to a portfolio, regardless of which further columns are used to create a portfolio.
        GROUPBY_COLUMNS = ['task', 'property', 'expected', 'sources'] + list(columns_same_within_portfolio)
        CALCULATED_COLUMNS = {"returned", "solver_walltime", "runtime", "timeout"}
        all_columns = set(df.columns)
        missing_columns = all_columns.difference(
                set(columns_merged_within_portfolio.keys()),
                set(columns_ignored),
                set(GROUPBY_COLUMNS),
                CALCULATED_COLUMNS
        )
        assert not missing_columns, f"These columns are not accounted for in either groupby, portfolio_columns, or ignored_columns: {missing_columns}"

        # Calculate the portfolio by grouping by the specified columns
        portfolios = df.groupby(GROUPBY_COLUMNS)

        def serial_portfolio(portfolio: pd.DataFrame):
            # TODO: they should be pre-ordered!
            portfolio.sort_values(['base_config', 'config'], inplace=True)
            cumulative_runtime = 0
            cumulative_solver_walltime = 0
            timeout = False
            returned = "unknown"
            for row in portfolio.itertuples():
                cumulative_runtime += row.runtime
                cumulative_solver_walltime += row.solver_walltime
                if row.timeout or cumulative_runtime > TIMEOUT:
                    timeout = True
                    cumulative_runtime = cumulative_solver_walltime = TIMEOUT
                    break

                returned = row.returned
                if returned in ('true', 'false'):
                    break

            return {
                "timeout": timeout, 
                "runtime": cumulative_runtime,
                "solver_walltime": cumulative_solver_walltime,
                "returned": returned
            }

        def parallel_walltime_portfolio(portfolio: pd.DataFrame):
            with_verdict = portfolio[portfolio['returned'].isin(('true', 'false'))]
            if not with_verdict.empty:
                fastest = with_verdict.loc[with_verdict['runtime'].idxmin()]
                return {
                    "timeout": False,
                    "runtime": fastest['runtime'],
                    "solver_walltime": fastest['solver_walltime'],
                    "returned": fastest['returned'],
                }
            timeout = bool(portfolio['timeout'].any()) or portfolio['runtime'].max() >= TIMEOUT
            return {
                "timeout": timeout,
                "runtime": TIMEOUT if timeout else portfolio['runtime'].max(),
                "solver_walltime": TIMEOUT if timeout else portfolio['solver_walltime'].max(),
                "returned": "unknown",
            }

        def calculate_portolio_result(keys, portfolio):
            result = dict(zip(GROUPBY_COLUMNS, keys if isinstance(keys, tuple) else (keys,)))
            result.update(columns_merged_within_portfolio)
            result.update(parallel_walltime_portfolio(portfolio))
            return result

        df_portfolio = pd.DataFrame([calculate_portolio_result(keys, group) for keys, group in portfolios])
        df_extended = pd.concat([df, df_portfolio], ignore_index=True)
        return df_extended

    return (make_portfolio,)


@app.cell
def _(df_raw, make_portfolio, portfolio_type, virtual_results_enabled):
    df_extended = df_raw.copy()
    if virtual_results_enabled.value:
        # df_extended = make_portfolio(df_extended, {"base_config",}, {"config": "portfolio_solver"}, {"rhs_evals",}, portfolio_type.value)
        df_extended = make_portfolio(df_extended, {"config",}, {"base_config": "portfolio"}, {"rhs_evals",}, portfolio_type.value)

    df_extended
    return (df_extended,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Filters
    """)
    return


@app.cell
def _(all_base_configs, all_sources, df_extended, mo):
    source_selector = mo.ui.dropdown(all_sources, label="Source")

    new_all_base_configs = sorted(df_extended['base_config'].dropna().unique())
    base_config_selector = mo.ui.dropdown(new_all_base_configs, value=all_base_configs[0] if all_base_configs else None, label="Base config")
    exclude_false_expected = mo.ui.checkbox(label="Exclude expected=false tasks", value=False)

    min_runtime_filter = mo.ui.number(value=0, label="Min runtime (s)")

    mo.vstack([mo.hstack([source_selector, base_config_selector, exclude_false_expected]), min_runtime_filter])
    return (
        base_config_selector,
        exclude_false_expected,
        min_runtime_filter,
        source_selector,
    )


@app.cell
def _(
    base_config_selector,
    df_extended,
    exclude_false_expected,
    min_runtime_filter,
    mo,
    source_selector,
):
    df = df_extended[df_extended['base_config'] == base_config_selector.value].copy()
    if exclude_false_expected.value:
        df = df[df['expected'].astype(str).str.lower() != 'false']
    if source_selector.value:
        df = df[df['sources'].fillna('').apply(
            lambda s: source_selector.value in [x.strip() for x in s.split('|')]
        )]
    if min_runtime_filter.value:
        passing = df.groupby(['task', 'property'])['runtime'].max()
        passing = passing[passing >= min_runtime_filter.value].index
        df = df.set_index(['task', 'property']).loc[passing].reset_index()

    mo.stop(df.empty, mo.callout(mo.md("No tasks match the current filters."), kind="warn"))
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Verdict Summary
    """)
    return


@app.cell
def _(df):
    def classify_verdict(row):
        returned = str(row['returned']).lower()
        expected = str(row['expected']).lower()
        has_verdict = returned in ('true', 'false')
        if not has_verdict:
            return 'unknown'
        if returned == expected:
            return 'right'
        else:
            return 'wrong'

    df['verdict'] = df.apply(classify_verdict, axis=1)
    return


@app.cell
def _(df):
    summary = (
        df.groupby(['base_config', 'config', 'verdict'])
          .size()
          .unstack(fill_value=0)
          .reindex(columns=['right', 'wrong', 'unknown'], fill_value=0)
    )
    timeouts = df[df['timeout'] == True].groupby(['base_config', 'config']).size().rename('timeout')
    summary = summary.join(timeouts, how='left').fillna(0).astype(int)

    summary
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Wrong Verdicts
    """)
    return


@app.cell
def _(df, mo):
    _wrong = df[df['verdict'] == 'wrong'][['task', 'property', 'base_config', 'config', 'expected', 'returned']]
    mo.callout(mo.md("All verdicts were correct."), kind="success") if _wrong.empty else mo.vstack([
        mo.callout(mo.md(f"**{len(_wrong)} wrong verdict(s):**"), kind="danger"),
        _wrong,
    ])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Cactus plot (Runtime / Correct verdicts)
    """)
    return


@app.cell
def _(df, mo, plt):
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

    cactus_plot(df, "runtime", "Runtime")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Compare Solvers
    """)
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
        df.sort_values(['task', 'property', 'base_config', 'config'], inplace=True)

        def format_float(s, digits=2):
            return f"{s:.{digits}f}"

        def add_relative_fields(af, bf, *fields):
            af = af.copy()
            bf = bf.copy()

            for field in fields:
                af[field + "_relative"] = af[field] / bf[field]
                bf[field + "_relative"] = bf[field] / af[field]

            return af, bf

        def geomean(s):
            return np.exp(np.log(s).mean())


        a_full_raw = df[df['config'] == cfg_a].set_index(key)
        b_full_raw = df[df['config'] == cfg_b].set_index(key)


        a_full, b_full = add_relative_fields(a_full_raw, b_full_raw, "solver_walltime", "rhs_evals", "runtime")
        a_full.sort_index(inplace=True)
        b_full.sort_index(inplace=True)

        diff = a_full.index.symmetric_difference(b_full.index)
        if not diff.empty:
            print(f"Index mismatch ({len(diff)} entries):", diff.tolist()[:10])
        a_dups = a_full_raw.index.duplicated().sum()
        b_dups = b_full_raw.index.duplicated().sum()
        if a_dups or b_dups:
            print(f"Duplicate index entries: {cfg_a}={a_dups}, {cfg_b}={b_dups}")
            print(a_full_raw[a_full_raw.index.duplicated(keep=False)].head(5))
        else:
            print("its fine")

        a_timeout = a_full.index[a_full['timeout']]
        b_timeout = b_full.index[b_full['timeout']]

        a_terminated = a_full.index.difference(a_timeout)
        b_terminated = b_full.index.difference(b_timeout)

        a_right = a_full.index[a_full['verdict'] == 'right']
        b_right = b_full.index[b_full['verdict'] == 'right']

        a_unknown = a_full.index[a_full['verdict'] == 'unknown']
        b_unknown = b_full.index[b_full['verdict'] == 'unknown']

        a_terminated_unknown = a_terminated.intersection(a_unknown)
        b_terminated_unknown = b_terminated.intersection(b_unknown)

        stats = {
            "right - both": len(a_right.intersection(b_right)),
            "timeout — both": len(a_timeout.intersection(b_timeout)),
            f"{cfg_a} right, {cfg_b} timeout": len(a_right.intersection(b_timeout)),
            f"{cfg_a} right, {cfg_b} terminated unknown": len(a_right.intersection(b_terminated_unknown)),
            f"{cfg_b} right, {cfg_a} timeout": len(b_right.intersection(a_timeout)),
            f"{cfg_b} right, {cfg_a} terminated unknown": len(b_right.intersection(a_terminated_unknown)),
        }


        def make_results_table(df_a, df_b):
            def make_column(df):
                return pd.Series({
                    'geomean relative runtime': format_float(geomean(df['runtime_relative'])),
                    'geomean relative solver walltime': format_float(geomean(df['solver_walltime_relative'])),
                    'geomean relative #rhs': format_float(geomean(df['rhs_evals_relative'])),
                    'median relative solver walltime': format_float(df['solver_walltime_relative'].median()),
                    'median relative #rhs': format_float(df['rhs_evals_relative'].median()),
                })
            return pd.DataFrame({
                cfg_a: make_column(df_a),
                cfg_b: make_column(df_b),
            })

        both_terminated = a_terminated.intersection(b_terminated)

        # From now on, we are only looking at cases where no solver timed out
        df_a_both_right = a_full.loc[a_right.intersection(both_terminated).intersection(b_right)]
        df_b_both_right = b_full.loc[b_right.intersection(both_terminated).intersection(a_right)]

        df_a_both_terminated_unknown = a_full.loc[a_terminated_unknown.intersection(both_terminated).intersection(b_unknown)]
        df_b_both_terminated_unknown = b_full.loc[b_terminated_unknown.intersection(both_terminated).intersection(a_unknown)]

        same_verdict = a_full['verdict'] == b_full['verdict'] 
        df_a_terminated_with_same_verdict = a_full.loc[both_terminated].loc[same_verdict]
        df_b_terminated_with_same_verdict = b_full.loc[both_terminated].loc[same_verdict]

        tables = [
            (f"Both correct (n={len(df_a_both_right)})", make_results_table(df_a_both_right, df_b_both_right)),
            (f"Both unknown, no timeout (n={len(df_a_both_terminated_unknown)})", make_results_table(df_a_both_terminated_unknown, df_b_both_terminated_unknown)),
            (f"Both same verdict, no timeout (n={len(df_a_terminated_with_same_verdict)})", make_results_table(df_a_terminated_with_same_verdict, df_b_terminated_with_same_verdict)),
        ]
        return tables, stats

    return (compare_solvers,)


@app.cell
def _(df, mo):
    _configs = sorted(df['config'].dropna().unique())
    cfg_a_selector = mo.ui.dropdown(_configs, value=_configs[0] if _configs else None, label="Config A")
    cfg_b_selector = mo.ui.dropdown(_configs, value=_configs[1] if len(_configs) > 1 else _configs[0] if _configs else None, label="Config B")
    mo.hstack([cfg_a_selector, cfg_b_selector])
    return cfg_a_selector, cfg_b_selector


@app.cell
def _(cfg_a_selector, cfg_b_selector, compare_solvers, df, mo):
    _tables, _stats = compare_solvers(df, cfg_a_selector.value, cfg_b_selector.value)
    mo.vstack([
        mo.hstack([mo.stat(label=k, value=str(v)) for k, v in _stats.items()]),
        *[item for title, table in _tables for item in [mo.md(f"### {title}"), table]],
    ])
    return


if __name__ == "__main__":
    app.run()
