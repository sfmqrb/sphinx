import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np
from matplotlib.ticker import LogLocator, NullFormatter
from consts import amp2

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
font_ampl = 1.4 * amp2
# only consider entries greater than this threshold.
from_ = 3000

# folder containing the csv files
home_dir = os.environ.get("HOME")
folder = os.path.join(home_dir, "research/sphinx-review/benchmark/data-extra-bits")

# define the csv files (adjust the names if needed)
files = {
    "extrabits0": os.path.join(folder, "benchmark_Fleck_ExtraBits1.csv"),
    "extrabits2": os.path.join(folder, "benchmark_Fleck_ExtraBits2.csv"),
    "extrabits4": os.path.join(folder, "benchmark_Fleck_ExtraBits4.csv"),
    "extrabits6": os.path.join(folder, "benchmark_Fleck_ExtraBits6.csv"),
    "infinifilter": os.path.join(folder, "benchmark_InfiniFilter.csv"),
}

# mapping of dataset keys to legend labels (using fleck-{i})
legend_names = {
    "extrabits0": "Sphinx",
    "extrabits2": "Sphinx-2",
    "extrabits4": "Sphinx-4",
    "extrabits6": "Sphinx-6",
    "infinifilter": "Aleph",
}

# function to read csv if it exists, otherwise return None


def read_csv_if_exists(file_path):
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    else:
        print(f"file {file_path} does not exist. skipping...")
        return None


# read all csv files into a dictionary of dataframes
datasets = {}
for label, file_path in files.items():
    df = read_csv_if_exists(file_path)
    if df is not None:
        # filter data so that only rows with num_entries > from_ are kept
        datasets[label] = df

# New, darker color palette for extrabits (a range of darker blues)
colors = {
    "extrabits0": "#00441B",  # very dark green
    "extrabits2": "#006D2C",
    "extrabits4": "#238B45",
    "extrabits6": "#41AE76",
    "infinifilter": "#000000",
}

# Use the same marker for extrabits (hollow circle) and a distinct one for infinifilter
markers = {
    "extrabits0": "o",
    "extrabits2": "o",
    "extrabits4": "o",
    "extrabits6": "o",
    "infinifilter": "s",
}

# Define distinct linestyles for each dataset to aid differentiation (especially for colorblind users)
linestyles = {
    "extrabits0": "-.",  # solid
    "extrabits2": "--",  # dashed
    "extrabits4": "-",  # dash-dot
    "extrabits6": ":",  # dotted
    "infinifilter": "-",  # or you can choose another style if preferred
}


def sample_points_by_log2(x_array, y_array, offset, rate):
    n = len(x_array)
    diff = np.diff(np.floor(np.log2(x_array) * rate + offset))
    idx = np.where(diff != 0)[0] + 1
    # if idx.size == 0 or idx[-1] != n - 1:
    #     idx = np.append(idx, n - 1)
    print(f"Number of samples: {len(idx)}")
    return x_array[idx], y_array[idx]


def smooth_data_insertion_with_expansion(data, column):
    x = data["num_entries"]
    y = data[column]
    bins = np.floor(np.log2(x)).astype(int)
    df_bins = pd.DataFrame({"bin": bins, "y": y})
    grouped = df_bins.groupby("bin")["y"].mean().reset_index()
    grouped["num_entries"] = 2 ** (grouped["bin"] + 1)
    grouped = grouped[grouped["num_entries"] > from_]
    # moving average
    grouped["y"] = grouped["y"].rolling(window=3, min_periods=1).mean()
    return grouped["num_entries"], grouped["y"]


def smooth_data(data, column, window_size, label, factor=3):
    roll_mean = data[column].rolling(window=window_size, min_periods=1).mean()
    roll_std = data[column].std()
    mask = (data[column] - roll_mean).abs() <= factor * roll_std
    filtered = data[column].where(mask)
    filtered_filled = filtered.interpolate(limit_direction="both")
    smoothed = filtered_filled.rolling(window=window_size, min_periods=1).mean()
    return smoothed


def local_extrema(data, col, label):
    min_entries = 0
    x = data["num_entries"]
    first_index = x[x > min_entries].index.min()
    win = 1
    mask1 = (data[col] >= data[col].shift(win)) & (data[col] >= data[col].shift(-win))
    win = 2
    mask2 = (data[col] >= data[col].shift(win)) & (data[col] >= data[col].shift(-win))
    mask = mask1 & mask2
    mask.iloc[first_index] = True
    mask.iloc[:first_index] = False
    mask.iloc[-1] = False
    extrema = data.loc[mask, col]
    x_ax = data.loc[mask, "num_entries"]
    return x_ax, extrema


def plot_metric(
    metric,
    ylabel,
    title,
    ax,
    datasets,
    skip=1,
    smoothing_window=1,
    no_smooth=True,
    y_limit=None,
    marker_size=8,
):
    for label, data in datasets.items():
        if "memory" in metric:
            x_ax, plot_data = local_extrema(data, metric, label)
        elif metric == "insertion_time":
            x_ax, plot_data = smooth_data_insertion_with_expansion(data, metric)
        else:
            plot_data = smooth_data(data, metric, smoothing_window, label)
            x_ax, plot_data = sample_points_by_log2(
                data["num_entries"], plot_data, 0.3, 1
            )
        from_index = np.asarray(x_ax).ravel().searchsorted(from_)
        plot_data = plot_data[from_index:]
        if metric != "memory":
            plot_data = plot_data / 1000
        x_ax = x_ax[from_index:]

        ax.plot(
            x_ax,
            plot_data,
            label=legend_names.get(label, label),  # use mapped legend name
            color=colors[label],
            marker=markers[label],
            markerfacecolor="none",  # hollow markers
            markersize=marker_size * 1.5,
            linestyle=linestyles[label],  # use the new linestyle
            linewidth=2,  # optionally, thicker lines for better visibility
            alpha=1,
        )

    if y_limit:
        ax.set_ylim(0, y_limit)
    else:
        ax.set_ylim(0)
    # set ticks all powers of 10 from 10**4 to 10**8

    ax.grid(False)  # remove grid lines
    ax.set_xlabel("#entries", fontsize=font_ampl * 18)  # x-axis not bold
    ax.set_ylabel(ylabel, fontsize=font_ampl * 16, fontweight="bold")  # y-axis bold
    ax.yaxis.set_label_coords(-0.16, 0.43)
    # set x-axis to logarithmic scale
    ax.tick_params(axis="both", which="major", labelsize=16 * font_ampl)
    ax.set_xscale("log")
    ticks = [10**i for i in range(4, 9)]
    ax.set_xticks(ticks)


# create a 1x3 grid of subplots for insertion time, query time, and non-existent query time
fig, axes = plt.subplots(1, 4, figsize=(24, 5), sharey=False)
smoothing_window = 20
skip = -1
MARKER_SIZE = 10

plot_metric(
    "memory",
    "max memory (bits/entry)",
    "nothing here",
    axes[0],
    datasets,
    skip=skip,
    smoothing_window=smoothing_window,
    no_smooth=False,
    marker_size=MARKER_SIZE,
)


plot_metric(
    "insertion_time",
    "avg. insertion lat. (μs)",
    "insertion time vs # entries",
    axes[1],
    datasets,
    skip=skip,
    smoothing_window=smoothing_window,
    no_smooth=False,
    marker_size=MARKER_SIZE,
)
plot_metric(
    "non_exs_query_time",
    # microsecond
    "avg. non-ex. query lat. (μs)",
    "non-existent query time vs # entries",
    axes[2],
    datasets,
    skip=skip,
    smoothing_window=smoothing_window,
    no_smooth=False,
    marker_size=MARKER_SIZE,
)
plot_metric(
    "tail-99-non-exs",
    # microsecond
    "tail-99 non-ex. query lat. (μs)",
    "non-existent query time vs # entries",
    axes[3],
    datasets,
    skip=skip,
    smoothing_window=smoothing_window,
    no_smooth=False,
    marker_size=MARKER_SIZE,
)


# add a bit more space on top of y-axis in the middle diagram
ylim_mid = axes[1].get_ylim()


# create a single legend for all curves and place it at the top center
lines, labels = axes[0].get_legend_handles_labels()
fig.legend(
    lines,
    labels,
    loc="upper center",
    ncol=len(datasets),
    fontsize=font_ampl * 18,
    frameon=False,
    bbox_to_anchor=(0.52, 1.07),
)

# remove top and right spines for each axis
for ax in axes:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

for ax in axes:
    ax.margins(0.023)

# Set margins similar to benchmark plots
plt.tight_layout(rect=[0, 0, 1, 0.87])
plt.savefig("extra_bits.svg")
