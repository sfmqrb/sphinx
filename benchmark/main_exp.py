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
    "A-memory",
    "B-memory",
    "C-memory",
    "D-memory",
    "E-memory",
    "F-memory",
    "A-Optane",
    "B-Optane",
    "C-Optane",
    "D-Optane",
    "E-Optane",
    "A-SSD",
    "B-SSD",
    "C-SSD",
    "D-SSD",
    "E-SSD",
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


def smooth_data(data, col, window_size):
    data["num_entries_shifted"] = data["num_entries"] - data["num_entries"].shift(
        window_size
    )
    data["num_entries_shifted"] = data["num_entries_shifted"].fillna(0)
    data["num_entries_shifted1"] = data["num_entries"] - data["num_entries"].shift(1)
    data["num_entries_shifted1"] = data["num_entries_shifted1"].fillna(0)
    data["aggregated"] = data["num_entries_shifted1"] * data[col]
    data["aggregated_sum"] = (
        data["aggregated"].rolling(window=window_size, min_periods=window_size).sum()
    )
    data["aggregated_col"] = data["aggregated_sum"] / data["num_entries_shifted"]
    return data["aggregated_col"]


def smooth_data_insertion_with_expansion(data, column):
    x = data["num_entries"]
    y = smooth_data(data, column, 5)
    # 1.1 is here to make sure each bin has at least one expansion
    # you can set it to 1.0 if you don't care about the very last
    # bin having no expansion but it will be less accurate
    bins = np.floor(np.log2(x * 1.1)).astype(int)
    df_bins = pd.DataFrame({"bin": bins, "y": y, "num_entries": x})
    grouped = df_bins.groupby("bin")["y"].mean().reset_index()
    grouped["num_entries"] = 2 ** (grouped["bin"] + 1)
    grouped = grouped[grouped["num_entries"] > min_entries]
    # moving average
    # grouped["y"] = grouped["y"].rolling(window=1, min_periods=1).mean()
    return grouped["num_entries"], grouped["y"]

def smooth_data_insertion_99th_perc(data, column, min_entries=0):
    x = data["num_entries"]
    y = smooth_data(data, column, 5)
    # 1.1 is here to make sure each bin has at least one expansion
    # you can set it to 1.0 if you don't care about the very last
    # bin having no expansion but it will be less accurate
    bins = np.floor(np.log2(x * 1.1)).astype(int)
    df_bins = pd.DataFrame({"bin": bins, "y": y, "num_entries": x})

    # select the 99th percentile of y in each bin instead of the mean
    grouped = (
        df_bins
        .groupby("bin")["y"]
        .quantile(0.99)
        .reset_index(name="y")
    )

    grouped["num_entries"] = 2 ** (grouped["bin"] + 1)
    grouped = grouped[grouped["num_entries"] > min_entries]

    return grouped["num_entries"], grouped["y"]

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
    default_font_size=-1,
):
    if default_font_size < 0:
        default_font_size = 18.5
    all_y_vals = []  # To collect all y-values from all datasets
    for label, data in datasets.items():
        offset = def_offset
        rate = def_rate
        window_final = smoothing_window
        if label == "DHT-expandable" and metric in ["memory", "FPR"]:
            continue
        # if label == "Aleph" and metric == "tail-99-i":
        #     continue
        if metric == "insertion_time":
            no_smooth = True
        plot_data = (
            data[metric]
            if no_smooth
            else smooth_data(data, metric, window_final)
        )

        x_vals = data["num_entries"].to_numpy()
        y_vals = pd.Series(plot_data).to_numpy()
        if metric == "insertion_time":
            x_vals, y_vals = smooth_data_insertion_with_expansion(data, metric)
        elif metric == "tail-99-i":
            x_vals, y_vals = smooth_data_insertion_99th_perc(data, metric, min_entries)
        else:
            x_vals, y_vals = sample_points_by_log2(x_vals, y_vals, offset, rate, metric)

        # Convert time metrics from nanoseconds to microseconds
        if metric in [
            "query_time",
            "tail-99-q",
            "update_time",
            "tail-99-u",
            "insertion_time",
            "tail-99-i",
        ]:
            y_vals = y_vals / 1000.0

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
    ax.set_ylabel(ylabel, fontsize=font_ampl * default_font_size, fontweight="bold")


    if ylabel_fix:
        ax.yaxis.set_label_coords(-0.14, 0.43)
    ax.set_xscale("log")
    if show_all_ticks:
        ax.set_xticks(
            [10**i for i in range(5, 9)], minor=False
        )
    ax.tick_params(axis="both", which="major", labelsize=default_font_size * font_ampl)

    if mode == "memory":
        ax.tick_params(axis="y", labelrotation=90)



# Generate Optane+SSD Figure (2 rows, 4 columns)
fig_ossd, axes_ossd = plt.subplots(2, 4, figsize=(30, 11))
modes = ["optane", "ssd"]
metrics = ["query_time", "tail-99-q", "update_time", "tail-99-u"]
for row, mode in enumerate(modes):
    base_path = os.path.join(
        home_dir, "research", proj_name, "benchmark", f"data-{mode}"
    )
    datasets = {
        label: pd.read_csv(os.path.join(base_path, file))
        for label, file in data_files.items()
        if os.path.exists(os.path.join(base_path, file))
    }
    start_idx = 5 if mode == "optane" else 10
    for col in range(4):
        plot_metric(
            metrics[col],
            [
                "avg. query lat. (µs)",
                "tail-99 query lat. (µs)",
                "avg. update lat. (µs)",
                "tail-99 update lat. (µs)",
            ][col],
            axes_ossd[row, col],
            datasets,
            marker_size=18,
            ylabel_fix=True,
            show_all_ticks=True,
        )
        idx = start_idx + col
        axes_ossd[row, col].set_xlabel(
            f"({alphbets[idx]}) #entries", fontsize=font_ampl * 19
        )
        axes_ossd[row, col].spines[["top", "right"]].set_visible(False)

lines, labels = axes_ossd[0, 1].get_legend_handles_labels()
fig_ossd.legend(
    lines, labels, loc="upper center", ncol=7, fontsize=font_ampl * 20, frameon=False,
    bbox_to_anchor=(0.5, 1.03)
)
fig_ossd.tight_layout(rect=[0, 0, 1, 0.97])
fig_ossd.savefig("benchmark_plots_main_optane_ssd.svg")
plt.close(fig_ossd)
