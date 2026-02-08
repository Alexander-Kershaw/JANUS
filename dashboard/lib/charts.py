from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

# Chart library to call in dashboards

def line_chart(
    df: pd.DataFrame,
    x: str,
    y_cols: list[str],
    title: str,
    xlabel: str | None = None,
    ylabel: str | None = None,
) -> plt.Figure:
    fig, ax = plt.subplots()
    for col in y_cols:
        ax.plot(df[x], df[col], label=col)
    ax.set_title(title)
    ax.set_xlabel(xlabel or x)
    ax.set_ylabel(ylabel or "")
    ax.legend()
    fig.autofmt_xdate()
    return fig


def pivot_line_chart(
    df: pd.DataFrame,
    x: str,
    category: str,
    value: str,
    title: str,
    xlabel: str | None = None,
    ylabel: str | None = None,
) -> plt.Figure:
    wide = (
        df.pivot_table(index=x, columns=category, values=value, aggfunc="sum")
        .fillna(0)
        .reset_index()
    )

    fig, ax = plt.subplots()
    for col in [c for c in wide.columns if c != x]:
        ax.plot(wide[x], wide[col], label=str(col))
    ax.set_title(title)
    ax.set_xlabel(xlabel or x)
    ax.set_ylabel(ylabel or "")
    ax.legend()
    fig.autofmt_xdate()
    return fig
