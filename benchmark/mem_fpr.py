import os
import math
import hashlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from consts import amp2, proj_name

sns.set_theme(
    style="whitegrid",
    rc={
        "font.family": "serif",
        "font.serif": ["Times New Roman"],
        "font.size": 16,
        "text.usetex": False,
        "mathtext.fontset": "custom",
        "mathtext.rm": "Times New Roman",
    },
)

min_entries = 20000
home_dir = os.environ.get("HOME")
font_ampl = 1.5 * amp2

alphbets = [
    "A-memory",
    "B-memory",
    "C-memory",
    "D-memory",
    "A-Optane",
    "B-Optane",
    "C-Optane",
    "D-Optane",
    "A-SSD",
    "B-SSD",
    "C-SSD",
    "D-SSD",
]

mode = "memory"
# mode = 'optane'
# mode = 'ssd'

directory = ""
if mode == "memory":
    directory = "data-memory"
elif mode == "optane":
    directory = "data-optane"
elif mode == "ssd":
    directory = "data-ssd"

def_offset = 0.3
def_rate = 1

base_path = os.path.join(home_dir, "research", "sphinx", "benchmark", directory)
data_files = {
    "Sphinx": "benchmark_Sphinx.csv",
    "Sphinx-Loop": "benchmark_Sphinx-Loop.csv",
    "DHT": "benchmark_DHT.csv",
    "Aleph": "benchmark_InfiniFilter.csv",
    "Collis-Free Aleph": "benchmark_Perfect_HT.csv",
    "RSQF": "benchmark_RSQF.csv",
    # "RSQF-static": "benchmark_RSQF-Static.csv",
}

datasets = {
    label: pd.read_csv(os.path.join(base_path, file))
    for label, file in data_files.items()
    if os.path.exists(os.path.join(base_path, file))
}

colors = {
    "Sphinx": "#006400",
    "Sphinx-Loop": "#B8860B",
    "DHT": "#00008B",
    "Aleph": "#000000",
    "Collis-Free Aleph": "#8B0000",
    "RSQF": "#4B0082",
    "RSQF-static": "#2F4F4F",
}

markers = {
    "Sphinx": "o",
    "Sphinx-3": "X",
    "Sphinx-Loop": "^",
    "DHT": "*",
    "Aleph": "s",
    "Collis-Free Aleph": "D",
    "RSQF": "v",
    "RSQF-static": "P",
}


def sample_points_by_log2(x_array, y_array, offset, rate, metric):
    n = len(x_array)
    start = np.searchsorted(x_array, min_entries, side="left")
    start2 = np.searchsorted(x_array, 2000, side="left")
    diff = np.diff(np.floor(np.log2(x_array) * rate + offset))
    idx = np.where(diff != 0)[0] + 1
    if metric != "FPR":
        idx = idx[idx >= start + 1]
    else:
        idx = idx[idx >= start2]
    print(f"Number of samples: {len(idx)}")
    return x_array[idx], y_array[idx]


def smooth_data(data, col, window_size, label, factor=100):
    return (
        data[col].rolling(window=window_size, min_periods=1)
        .mean()
    )


def detect_highs_plus_first_datapoint(data, col):
    y = data[col]
    x = data["num_entries"]
    high_mask = (y.shift(1) <= y) & (y.shift(-1) <= y)
    indices = high_mask[high_mask].index
    x = x.loc[indices]
    y = y.loc[indices]
    return x, y


def local_extrema(data, col, label):
    global min_entries
    tmp_min_entries = min_entries
    min_entries = 4000
    win = 1
    x = data["num_entries"]
    first_index = x[x > min_entries].index.min()
    mask = (
        (data[col] >= data[col].shift(win)) & (data[col] >= data[col].shift(-win))
    ) | ((data[col] <= data[col].shift(win)) & (data[col] <= data[col].shift(-win)))
    mask.iloc[first_index] = True
    mask.iloc[:first_index] = False
    mask.iloc[-1] = True
    if label == "Aleph" and mode == "memory":
        mask.iloc[-1] = False
    extrema = data.loc[mask, col]
    x_ax = data.loc[mask, "num_entries"]
    if label == "Collis-Free Aleph":
        extrema = extrema.loc[extrema.index >= first_index].reset_index(drop=True)
        x_ax = x_ax.loc[x_ax.index >= first_index].reset_index(drop=True)
        new_indices = []
        for i in range(len(extrema)):
            start = max(0, i - 2)
            end = min(len(extrema), i + 3)
            window = extrema.iloc[start:end].values
            candidate = extrema.iloc[i]
            local_idx = i - start
            neighbors = np.delete(window, local_idx)
            if (candidate > neighbors[0] and candidate >= neighbors[1]) or (
                candidate < neighbors[-1] and candidate <= neighbors[-2]
            ):
                new_indices.append(i)
        extrema = extrema.iloc[new_indices]
        x_ax = x_ax.iloc[new_indices]
        mask2 = ((extrema >= extrema.shift(1)) & (extrema >= extrema.shift(-1))) | (
            (extrema <= extrema.shift(1)) & (extrema <= extrema.shift(-1))
        )
        mask2.iloc[0] = True
        mask2.iloc[-1] = True
        extrema = extrema.loc[mask2]
        x_ax = x_ax.loc[mask2]
    min_entries = tmp_min_entries
    return x_ax, extrema


def plot_metric(
    metric,
    ylabel,
    ax,
    datasets,
    smoothing_window=5,
    no_smooth=False,
    y_limit=None,
    marker_size=6,
    show_all_ticks=False,
    ylabel_fix=False,
):
    for label, data in datasets.items():
        offset = def_offset
        rate = def_rate
        window_final = smoothing_window
        if label == "Sphinx-Loop" and metric == "memory":
            continue
        plot_data = (
            data[metric]
            if no_smooth
            else smooth_data(data, metric, window_final, label)
        )
        x_vals = data["num_entries"].to_numpy()
        y_vals = pd.Series(plot_data).to_numpy()
        if metric == "memory" and label not in ["DHT"]:
            x_vals, y_vals = local_extrema(data, metric, label)
        elif metric == "memory" and label == "DHT":
            x_vals, y_vals = sample_points_by_log2(
                x_vals, y_vals, 0.5, rate * 2, metric
            )
        else:
            x_vals, y_vals = sample_points_by_log2(x_vals, y_vals, offset, rate, metric)
        if metric == "FPR" and label == "Aleph":
            x_vals, y_vals = detect_highs_plus_first_datapoint(data, metric)
        if label == "Collis-Free Aleph":
            labelText = "Adaptive Aleph"
        else:
            labelText = label
        ax.plot(
            np.asarray(x_vals),
            np.asarray(y_vals),
            label=labelText,
            color=colors[label],
            marker=markers[label],
            markersize=marker_size,
            linestyle="-",
            alpha=0.9,
            markerfacecolor="none",
        )
    ax.grid(False)
    ax.margins(x=0.02)
    if y_limit:
        ax.set_ylim(0, y_limit)
    elif metric != "FPR":
        ax.set_ylim(0)
    if metric == "FPR":
        ax.set_yscale("symlog", linthresh=1e-6)
    ax.set_ylabel(ylabel, fontsize=font_ampl * 20, fontweight="bold")
    if ylabel_fix:
        ax.yaxis.set_label_coords(-0.13, 0.5)
    ax.set_xscale("log")
    # show all powers of 10
    if show_all_ticks:
        ax.set_xticks(
            [10 ** i for i in range(int(math.log10(min_entries)), 9)], minor=False
        )
    ax.tick_params(axis="both", which="major", labelsize=19 * font_ampl)


if __name__ == "__main__":
    # First figure (1 row, 2 columns)
    font_ampl *= 1.1
    fig1, axes1 = plt.subplots(1, 2, figsize=(15, 6), sharey=False)
    plot_metric(
        "memory",
        "memory (bits/entry)",
        axes1[0],
        datasets,
        smoothing_window=-1,
        no_smooth=True,
        y_limit=35,
        marker_size=13,
        show_all_ticks=True,
    )
    plot_metric(
        "FPR", "false positive rate", axes1[1], datasets, marker_size=13, no_smooth=True, show_all_ticks=True
    )

    for ax in axes1:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.set_xlabel("#entries", fontsize=font_ampl * 20)
        ax.xaxis.set_label_coords(0.5, -0.17)

    lines1, labels1 = axes1[0].get_legend_handles_labels()

    lines2 = lines1[:1] + lines1[2:]
    labels2 = labels1[:1] + labels1[2:]
    leg1 = fig1.legend(
        lines2,
        labels2,
        loc="upper center",
        bbox_to_anchor=(0.485, 1.08),
        ncol=4,
        fontsize=font_ampl * 20,
        frameon=False,
        # more packed 
        handletextpad=0.16,    # <<–– tighter spacing here
        labelspacing=0.1,      # vertical gap
        columnspacing=0.6,   # <<< add this to reduce horizontal gap
    )
    axes1[0].add_artist(leg1)
    fig1.legend(
        [lines1[1]],
        [labels1[1]],
        loc="upper center",
        bbox_to_anchor=(0.318, 0.98),
        ncol=1,
        fontsize=font_ampl * 20,
        frameon=False,
        handletextpad=0.16,    # <<–– tighter spacing here
        labelspacing=0.1,      # vertical gap
    )

    plt.tight_layout(rect=[0, 0, 1, 0.92])
    plt.subplots_adjust(
        top=0.89,
        bottom=0.19,
        left=0.102,
        right=0.98,
        wspace=0.33   # tighten between the two subplots
    )
    
    plt.savefig("benchmark_plots_mem_fpr.svg")
    

