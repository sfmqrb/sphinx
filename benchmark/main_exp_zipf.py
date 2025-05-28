import os
import math
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
def_offset = 0.3
def_rate = 1

alphbets = [
    "memory", "Optane"
]

data_files = {
    "Sphinx": "benchmark_Sphinx.csv",
    "Sphinx-4": "benchmark_Sphinx-ReserveBits4.csv",
    "DHT": "benchmark_DHT.csv",
    "DHT-expandable": "benchmark_Sphinx-Loop.csv",
    "Aleph": "benchmark_InfiniFilter.csv",
    "Collis-Free Aleph": "benchmark_Perfect_HT.csv",
    "RSQF": "benchmark_RSQF.csv",
}

colors = {
    "Sphinx": "#006400",
    "Sphinx-4": "#369596",
    "DHT": "#00008B",
    "DHT-expandable": "#B8860B",
    "Aleph": "#000000",
    "Collis-Free Aleph": "#8B0000",
    "RSQF": "#4B0082",
}

markers = {
    "Sphinx": "o",
    "Sphinx-4": "X",
    "DHT": "*",
    "DHT-expandable": "^",
    "Aleph": "s",
    "Collis-Free Aleph": "D",
    "RSQF": "v",
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
    return x_array[idx], y_array[idx]

def smooth_data(data, col, window_size, label, factor=100):
    data["num_entries_shifted"] = data["num_entries"] - data["num_entries"].shift(window_size)
    data["num_entries_shifted"] = data["num_entries_shifted"].fillna(0)
    data["num_entries_shifted1"] = data["num_entries"] - data["num_entries"].shift(1)
    data["num_entries_shifted1"] = data["num_entries_shifted1"].fillna(0)
    data["aggregated"] = data["num_entries_shifted1"] * data[col]
    data["aggregated_sum"] = data["aggregated"].rolling(window=window_size, min_periods=window_size).sum()
    data["aggregated_col"] = data["aggregated_sum"] / data["num_entries_shifted"]
    return data["aggregated_col"]


def plot_metric(
    metric,
    ylabel,
    ax,
    datasets,
    smoothing_window=20,
    no_smooth=False,
    y_limit=None,
    marker_size=6,
    show_all_ticks=False,
    ylabel_fix=False,
):
    all_y_vals = []  # To collect all y-values from all datasets
    for label, data in datasets.items():
        offset = def_offset
        rate = def_rate
        window_final = smoothing_window
        if label == "DHT-expandable" and metric in ["memory", "FPR"]:
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

        # Convert time metrics from nanoseconds to microseconds
        if metric in ["ops/sec"]:
            y_vals = ((10**9)/y_vals) / 10**3

        # Collect y-values for auto-scaling
        all_y_vals.extend(y_vals.tolist())

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
    ax.margins(x=0.03, y=0.06)
    if y_limit:
        ax.set_ylim(0, y_limit)
    elif metric != "FPR":
        y_max = max(all_y_vals)
        ax.set_ylim(0, y_max * 1.04)  # 10% padding
    if metric == "FPR":
        ax.set_yscale("symlog", linthresh=1e-6)
    if metric == "insertion_time" and mode == "ssd":
        ax.set_xlim(min_entries // 1.3, 10**8)
    ax.set_ylabel(ylabel, fontsize=font_ampl * 18, fontweight="bold")
    if ylabel_fix:
        ax.yaxis.set_label_coords(-0.16, 0.5)
    ax.set_xscale("log")
    if show_all_ticks:
        ax.set_xticks(
            [10 ** i for i in range(int(math.log10(min_entries)), 9)], minor=False
        )
    ax.tick_params(axis="both", which="major", labelsize=19 * font_ampl)

# Generate Memory Figure (5 columns)
fig_mem, axes_mem = plt.subplots(1, 5, figsize=(39, 7))
mode = "memory"
base_path = os.path.join(home_dir, "research", proj_name, "benchmark", "data-memory-zipf")
datasets = {
    label: pd.read_csv(os.path.join(base_path, file))
    for label, file in data_files.items()
    if os.path.exists(os.path.join(base_path, file))
}
metrics = ["ops/sec"]
fig_ossd, axes_ossd = plt.subplots(1, 2, figsize=(15, 8))
modes = ["memory", "optane"]
idx = 0
for row, mode in enumerate(modes):
    base_path = os.path.join(home_dir, "research", proj_name, "benchmark", f"data-{mode}-zipf")
    datasets = {
        label: pd.read_csv(os.path.join(base_path, file))
        for label, file in data_files.items()
        if os.path.exists(os.path.join(base_path, file))
    }
    for col in range(1):
        plot_metric(
            metrics[col],
            "avg. latency (µs)",
            axes_ossd[col + row], datasets, marker_size=18, ylabel_fix=True
        )
        axes_ossd[col + row].set_xlabel(f"({alphbets[idx]}) #entries", fontsize=font_ampl*19)
        axes_ossd[col + row].spines[["top", "right"]].set_visible(False)
        idx += 1

lines, labels = axes_ossd[0].get_legend_handles_labels()

# Adjust ncol depending on your total number of entries.
fig_ossd.legend(
    lines,
    labels,
    loc="upper center",
    ncol=3,  # change this based on how many entries you have
    fontsize=font_ampl * 20,
    frameon=False,
    bbox_to_anchor=(0.5, 1.03),  # push the legend upward slightly
)

# Adjust the layout to make room for the legend
fig_ossd.tight_layout(rect=[0, 0, 1, 0.78])

fig_ossd.savefig("benchmark_plots_main_zipf_mem_optane.svg")
plt.close(fig_ossd)
