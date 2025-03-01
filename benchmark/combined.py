import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker
import os
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

font_ampl = 1.25 * amp2
UP = 1.12
LEFT = -0.075
final_point = False

# Create a combined figure with two subplots (with specified width ratios)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5), gridspec_kw={'width_ratios': [3, 3]})

# ==============================
# First Diagram: Read Latency Speedup
# ==============================

csv_file = "data-skew/read_latency.csv"
data1 = pd.read_csv(csv_file)
colors_list = ["#8B0000", "#006400", "#00008B", "#8B008B"]
markers_list = ["o", "s", "D", "P"]

unique_buffer_sizes = sorted(data1["buffer_size_ratio"].unique())
unique_buffer_sizes.reverse()
for i, buff in enumerate(unique_buffer_sizes):
    df_buff = data1[data1["buffer_size_ratio"] == buff]
    df_grouped = (
        df_buff.groupby(["buffer_hit_ratio", "name"])["query_latency"]
        .mean()
        .reset_index()
    )
    pivot = df_grouped.pivot(
        index="buffer_hit_ratio", columns="name", values="query_latency"
    ).sort_index()
    pivot["speedup"] = pivot["Sphinx-Loop"] / pivot["Sphinx"]
    pivot["speedup"] = pivot["speedup"].rolling(window=1, min_periods=1).mean()
    mask = ((pivot.index * 100).round().astype(int)) % 5 >= 0
    if not final_point:
        mask[-1] = False
    pivot_filtered = pivot[mask]
    x_values = pivot_filtered.index * 100
    y_values = pivot_filtered["speedup"]
    ax1.plot(
        x_values,
        y_values,
        marker=markers_list[i % len(markers_list)],
        linestyle="-",
        markersize=17,
        markerfacecolor="none",
        color=colors_list[i % len(colors_list)],
        label=f"{buff*100:.0f}% buffer size",
    )

ax1.set_xlabel("(A) buffer hit (%)", fontsize=font_ampl * 20)
ax1.set_ylabel("Sphinx latency speedup", fontsize=font_ampl * 19, fontweight="bold")
ax1.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.2f"))
all_ticks = sorted(data1["buffer_hit_ratio"].unique())
filtered_ticks = [tick for tick in all_ticks if round(tick * 100) % 5 == 0]
ax1.set_xticks([tick * 100 for tick in filtered_ticks])
ax1.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x)}"))
ax1.set_ylim(1)
ax1.grid(False)
ax1.tick_params(axis="both", which="major", labelsize=20 * font_ampl)
ax1.margins(0.09)
ax1.set_xlim(58.5, 101)

# Place the legend above the plot (using bbox_to_anchor relative to the axes)
ax1.legend(
    loc="upper left",
    ncol=1,
    fontsize=font_ampl * 20,
    frameon=False,
    bbox_to_anchor=(0, UP),  # adjust the y-value (e.g. 1.05) to move the legend higher
)

ax1.spines["top"].set_visible(False)
ax1.spines["right"].set_visible(False)

# ==============================
# Second Diagram: Throughput vs Threads
# ==============================

csv_file = 'data-mt/throughput_vs_threads_ssd.csv'
data2 = pd.read_csv(csv_file)
data2["MaximumThroughput_ops_s"] = data2[["ReadThroughput_ops_s", "RandomReadThroughput_ops_s"]].max(axis=1)
colors = {
    "MaximumThroughput_ops_s": "#8B0000",
    "InsertThroughput_ops_s": "#00008B",
    "ReadThroughput_ops_s":   "#006400",
}
markers = {
    "MaximumThroughput_ops_s": "x",
    "InsertThroughput_ops_s": "^",
    "ReadThroughput_ops_s":   "h",
}
legend_names = {
    "InsertThroughput_ops_s": "Inserts",
    "ReadThroughput_ops_s":   "Queries",
    "MaximumThroughput_ops_s": "SSD random reads",
}

for col in ["MaximumThroughput_ops_s", "ReadThroughput_ops_s", "InsertThroughput_ops_s"]:
    ax2.plot(
        data2["ThreadCount"],
        data2[col],
        marker=markers[col],
        color=colors[col],
        linestyle='-',
        markersize=17,
        label=legend_names[col],
        markerfacecolor="none"
    )

ax2.set_xlabel("(B) thread count", fontsize=font_ampl * 20)
ax2.set_ylabel("throughput (ops/s)", fontsize=font_ampl * 19, fontweight="bold")
ax2.set_xscale("log")
ax2.set_yscale("log", base=2)
ax2.yaxis.set_major_locator(ticker.LogLocator(base=2))
ax2.yaxis.set_major_formatter(ticker.LogFormatterMathtext(base=2))
x_ticks = data2["ThreadCount"].tolist()
ax2.set_xticks(x_ticks)
ax2.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x)}'))
plt.setp(ax2.get_xticklabels(), rotation=0, ha="right")
ax2.grid(False)
ax2.tick_params(axis="both", which="major", labelsize=20 * font_ampl)

# Place the legend above the plot for the second diagram.
ax2.legend(
    loc="upper left",
    ncol=1,
    fontsize=font_ampl * 19,
    frameon=False,
    bbox_to_anchor=(LEFT, UP),  # adjust as needed (e.g. 1.05) to raise the legend
)

ax2.margins(x=0.05)
ax2.spines["top"].set_visible(False)
ax2.spines["right"].set_visible(False)

# Instead of tight_layout, use subplots_adjust to reserve room for legends without shrinking axes.
fig.subplots_adjust(top=0.9, bottom=0.19,left=0.102, wspace=0.35, right=0.98)

plt.savefig("combined_diagrams.svg")

