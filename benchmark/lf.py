import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.signal import savgol_filter
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
# -------------------------------
# PARAMETERS (feel free to adjust)
# -------------------------------
moving_avg_window = 10  # Smoothing window size (for Savitzky–Golay smoothing)
skip = 40                # Plot every nth point
data_dir = "data-lf"     # Directory where the CSV files are stored

# -------------------------------
# FILES and DATASET LABELS
# -------------------------------
files = {
    "Sphinx": "benchmark_Fleck.csv",
    "RSQF": "benchmark_RSQF-Static.csv",
    "Collis-Free Aleph": "benchmark_Perfect_HT.csv",
    "DHT": "benchmark_DHT.csv",
}

# -------------------------------
# Define Colors and Markers (DO NOT CHANGE)
# -------------------------------
colors = {
    "Sphinx": "#006400",         # Black
    "RSQF": "#4B0082",
    "Collis-Free Aleph": "#8B0000",    # Dark Red
    "DHT": "#00008B",           # Dark Blue
}

markers = {
    "Sphinx": "o",        # Circle -> will be hollowed below
    "RSQF": "v",  # Plus (filled) -> will be hollowed below
    "Collis-Free Aleph": "D",   # Diamond -> will be hollowed below
    "DHT": "*",          # Star -> will be hollowed below
}

# -------------------------------
# Smoothing Function with Outlier Removal
# -------------------------------


def smooth_data_remove_outliers(data, column, window_size, skip=1, start=0, factor=1, polyorder=2):
    # Compute rolling mean and standard deviation
    roll_mean = data[column].rolling(window=window_size, min_periods=1).mean()
    roll_std = data[column].rolling(window=window_size, min_periods=1).std()

    # Remove outliers (points deviating too much from the rolling mean)
    mask = (data[column] - roll_mean).abs() <= factor * roll_std
    filtered = data[column].where(mask)
    # Fill any missing values (from removed outliers) by interpolation
    filtered_filled = filtered.interpolate(limit_direction='both')

    # Determine window_length for Savitzky–Golay (must be odd)
    if window_size % 2 == 0:
        window_length = window_size + 1
    else:
        window_length = window_size
    if window_length > len(filtered_filled):
        # Ensure the window length is not larger than the data length
        window_length = len(filtered_filled) if len(
            filtered_filled) % 2 != 0 else len(filtered_filled) - 1

    # Apply Savitzky–Golay filter
    smoothed = savgol_filter(
        filtered_filled, window_length=window_length, polyorder=polyorder)
    res = smoothed[start::skip]
    return pd.Series(res).rolling(window=1, min_periods=1).mean().to_numpy()

# -------------------------------
# Function to Load and Preprocess Datasets
# -------------------------------


def load_datasets(data_dir, files):
    """
    Load each CSV file, filter out rows with LF < 0.1,
    compute the percentage of inserted keys, and only keep data
    for less than 100% inserted.

    Returns a dictionary mapping label -> processed DataFrame.
    """
    datasets = {}
    for label, filename in files.items():
        filepath = os.path.join(data_dir, filename)
        df = pd.read_csv(filepath)
        # Filter out rows where LF is too low
        df = df[df['LF'] >= 0.01]
        # Compute the percentage of inserted keys (using num_entries / 4096 * 100)
        df['perc_inserted'] = df['num_entries'] / 4096 * 100
        # Only keep data where the inserted percentage is below 100%
        datasets[label] = df[df['perc_inserted'] < 100].copy()
    return datasets

# -------------------------------
# Function to Plot a Single Metric
# -------------------------------


def plot_metric(ax, datasets, metric, x_col, smoothing_window, skip, apply_smoothing=True):
    """
    Plot a metric for all datasets on the given axis.

    Parameters:
      ax             : The matplotlib Axes to plot on.
      datasets       : Dictionary of datasets (label -> DataFrame).
      metric         : Name of the metric column to plot.
      x_col          : Name of the column for the x-axis.
      smoothing_window: Window size to use for smoothing.
      skip           : Plot every nth point.
      apply_smoothing: Whether to smooth the metric (True) or plot raw values (False).
    """
    for label, df in datasets.items():
        if apply_smoothing:
            # Smooth the data using our outlier-removal and Savitzky–Golay filter
            y_data = smooth_data_remove_outliers(
                df, metric, smoothing_window, skip=skip)
            if label == "DHT" and (metric == "insertion_time" or metric == "query_time"):
                y_data = smooth_data_remove_outliers(
                    df, metric, smoothing_window * 80, skip=skip)

            x_data = df[x_col].values[::skip]
        else:
            x_data = df[x_col].values[::skip]
            y_data = df[metric].values[::skip]

        # skip again
        x_data = x_data[::2]
        y_data = y_data[::2]
        if metric in ["insertion_time", "query_time"]:
            y_data = y_data / 1000
        if label == "Collis-Free Aleph":
            labelText = "Adaptive Aleph"
        else:
            labelText = label
        ax.plot(
            x_data,
            y_data,
            label=labelText,
            color=colors[label],
            marker=markers[label],
            markerfacecolor="none",  # Hollow markers
            markersize=18,
            linestyle='-',
            alpha=0.9
        )

    # Set common x-axis label
    ax.set_xlabel("inserted keys (%)", fontsize=font_ampl*18)

    # Set additional properties depending on the metric
    if metric == "insertion_time":
        ax.set_ylabel("avg. insert lat. (μs)",
                      fontsize=font_ampl*18, fontweight='bold')
        ax.set_xlim(left=-3)
        ax.set_ylim(bottom=0, top=2.1)
    elif metric == "query_time":
        ax.set_ylabel("avg. query lat. (μs)",
                      fontsize=font_ampl*18, fontweight='bold')
        ax.set_xlim(left=0)
        ax.set_ylim(bottom=0, top=3.4)
    elif metric == "memory":
        ax.set_ylabel("memory (bits/entry)",
                      fontsize=font_ampl*18, fontweight='bold')
        ax.set_xlim(left=0)
    elif metric == "LF":
        ax.set_ylabel("load factor", fontsize=font_ampl*18, fontweight='bold')
        ax.set_xlim(left=-3)
    if metric == "memory":
        ax.set_ylim(bottom=0, top=50)
    ax.tick_params(axis="both", which="major", labelsize=16*font_ampl)
    ax.grid(False)

# -------------------------------
# Main Function
# -------------------------------


def main():
    # Load the datasets from CSV files
    datasets = load_datasets(data_dir, files)

    # Create a 1x4 grid of subplots
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # Plot each metric:
    # 1. Insertion Time (smoothed)
    plot_metric(axes[0], datasets, "insertion_time", "perc_inserted",
                moving_avg_window, skip, apply_smoothing=True)

    # 4. Load Factor (raw; no smoothing)
    plot_metric(axes[1], datasets, "LF", "perc_inserted",
                moving_avg_window, skip, apply_smoothing=False)

    # Create a single legend for all curves, placed at the top center
    handles, labels_list = axes[0].get_legend_handles_labels()

    fig.legend(handles, labels_list, loc='upper center', ncol=4,
           fontsize=font_ampl*18, frameon=False, bbox_to_anchor=(0.5, 1.07),
           labelspacing=0.5,   # reduce vertical spacing between entries
           columnspacing=0.8)  # reduce horizontal spacing between columns
    # Remove top and right spines for each axis
    for ax in axes:
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
    plt.tight_layout(rect=[0, 0, 1, 0.90])
    plt.savefig("lf_benchmark_plot.svg")


# -------------------------------
# Run the Script
# -------------------------------
if __name__ == "__main__":
    main()
