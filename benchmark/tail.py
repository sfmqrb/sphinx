import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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
font_ampl = 2.4 * amp2
# Define base path (assumed to be the current directory based on file locations)
base_path = './data-tail' 

# Define filenames for SSD and Optane datasets
data_files_ssd = {
    "Sphinx": "fleck_query_times_ssd.csv",
    "Aleph": "infinifilter_query_times_ssd.csv",
}
data_files_optane = {
    "Sphinx": "fleck_query_times_optane.csv",
    "Aleph": "infinifilter_query_times_optane.csv",
}

# Load datasets if they exist (each CSV has one column of sorted query times)
datasets_ssd = {
    label: pd.read_csv(os.path.join(base_path, file), header=None, names=["query_time"])
    for label, file in data_files_ssd.items()
    if os.path.exists(os.path.join(base_path, file))
}
datasets_optane = {
    label: pd.read_csv(os.path.join(base_path, file), header=None, names=["query_time"])
    for label, file in data_files_optane.items()
    if os.path.exists(os.path.join(base_path, file))
}

def transform_percentile(p):
    """
    Transform a percentile value p (0 to 100) into a log-scaled x-coordinate.
    This mapping emphasizes the high percentiles.
    """
    p = p[p != 100]  # Avoid log(0)
    print(p[-1])
    return 2 - np.log10(100 - p)

# Define markers and colors (using academic-friendly colors)
markers = {"Sphinx": "o", "Aleph": "s", "Sphinx-Loop": "^"}
colors = {"Sphinx": "#006400", "Aleph": "#000000", "Sphinx-Loop": "#B8860B"}

# Create a figure with 1 row and 2 columns
fig, axes = plt.subplots(1, 2, figsize=(24, 8.8))

# Plot data on each subplot
for ax, (storage_type, datasets) in zip(axes, [("SSD", datasets_ssd), ("Optane", datasets_optane)]):
    for label, df in datasets.items():
        times = df["query_time"].values
        N = len(times)
        times = times[:-1]
        # Create percentiles from 0% to 100%
        percentiles = np.linspace(0, 100, N)
        x_vals = transform_percentile(percentiles)
        
        # Choose marker positions at these specific percentiles (up to 99.99)
        marker_percentiles = [50, 90, 99, 99.9, 99.99, 99.999]
        marker_indices = [int(p/100 * (N - 1)) for p in marker_percentiles]

        times = times / 1000  # Convert to microseconds
        
        ax.plot(x_vals, times,
                marker=markers.get(label, "o"),
                linestyle='-',
                markerfacecolor='none',  # hollow markers
                markersize=30,           # bigger markers
                markevery=marker_indices,
                color=colors.get(label, "black"),
                label=label)
    
    # Set x-axis ticks for specific percentiles (up to 99.99)
    tick_percentiles = [50, 90, 99, 99.9, 99.99, 99.999]
    tick_positions = transform_percentile(np.array(tick_percentiles))
    if storage_type == "Optane":
        ax.set_yticks([0, 1, 2, 3, 4, 5])
    if storage_type == "SSD":
        ax.set_yticks([0, 50, 100, 150, 200, 250])
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([f"{tp}%" for tp in tick_percentiles], fontsize=font_ampl*18)
    ax.set_xlabel("percentile (%)", fontsize=font_ampl*20)
    ax.set_ylabel(storage_type + " latency (μs)", fontsize=font_ampl*19, fontweight='bold')
    # move ylabel to the left
    ax.set_xlim(0.21, 5.15)  # End chart at 99.99 (max x = 2 - log10(0.01) = 4)
    
    # Remove top and right axes
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(False)
    ax.tick_params(axis="x", which="major", labelsize=18*font_ampl, rotation=30)
    ax.tick_params(axis="y", which="major", labelsize=18*font_ampl)
    ax.margins(x=0.02)  # Add some margin to the right

    # Add legend in the top left corner in one column
    ax.legend(loc='upper left', ncol=1, fontsize=font_ampl*20, frameon=False, bbox_to_anchor=(-0.05, 1.1))

axes[1].yaxis.set_label_coords(-0.1, 0.42)  # Adjust y-label position
axes[1].set_ylim(0, 5.2)
axes[0].set_ylim(0, 280.000)

plt.tight_layout()
plt.subplots_adjust(wspace=0.3)
plt.savefig("tail_latency_distribution.svg")

