import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker
from consts import amp2

# Set global style
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
UP = 1.22
LEFT = -0.070
final_point = False

# Create a combined figure with two subplots (with specified width ratios)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.7), gridspec_kw={'width_ratios': [3, 3]})

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
    mask = ((pivot.index * 100).round().astype(int)) % 5 >= 0
    if not final_point:
        mask[-1] = False
    pivot = pivot[mask]
    x = pivot.index * 100
    y = pivot["speedup"]
    ax1.plot(
        x, y,
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

filtered_ticks = [
    tick for tick in sorted(data1["buffer_hit_ratio"].unique())
    if round(tick * 100) % 5 == 0
]
ax1.set_xticks([t * 100 for t in filtered_ticks])
ax1.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x)}"))

ax1.set_ylim(1)
ax1.set_xlim(58.5, 101)
ax1.grid(False)
ax1.tick_params(axis="both", which="major", labelsize=20 * font_ampl)
ax1.margins(0.09)
ax1.spines["top"].set_visible(False)
ax1.spines["right"].set_visible(False)
ax1.legend(
    loc="upper left",
    ncol=1,
    fontsize=font_ampl * 22,
    frameon=False,
    bbox_to_anchor=(0, UP),
    handletextpad=0.3,    # <<–– tighter spacing here
    labelspacing=0.1      # vertical gap
)

# ==============================
# Second Diagram: Throughput vs Threads
# ==============================
data2 = pd.read_csv('data-mt/throughput_vs_threads_ssd.csv')
data3 = pd.read_csv('data-mt/throughput_vs_threads_4ReserveBitsssd.csv')
data2["MaximumThroughput_ops_s"] = data2["RandomReadThroughput_ops_s"]

colors = {
    "MaximumThroughput_ops_s": "#8B0000",
    "InsertThroughput_ops_s": "#00008B",
    "InsertThroughput4_ops_s": "#00000B",
    "ReadThroughput_ops_s":   "#006400",
}
markers = {
    "MaximumThroughput_ops_s": "x",
    "InsertThroughput_ops_s": "^",
    "InsertThroughput4_ops_s": "P",
    "ReadThroughput_ops_s":   "h",
}
legend_names = {
    "InsertThroughput_ops_s": "Sphinx Inserts",
    "InsertThroughput4_ops_s": "Sphinx-4 Inserts",
    "ReadThroughput_ops_s":   "Queries",
    "MaximumThroughput_ops_s": "SSD rand. reads",
}

# Plot Sphinx-4 inserts
ax2.plot(
    data3["ThreadCount"],
    data3["InsertThroughput_ops_s"],
    marker=markers["InsertThroughput4_ops_s"],
    color=colors["InsertThroughput4_ops_s"],
    linestyle='-',
    markersize=17,
    label=legend_names["InsertThroughput4_ops_s"],
    markerfacecolor="none"
)

# Plot the rest
for col in ["ReadThroughput_ops_s", "InsertThroughput_ops_s", "MaximumThroughput_ops_s", ]:
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

ax2.set_xticks(data2["ThreadCount"].tolist())
ax2.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x)}"))
plt.setp(ax2.get_xticklabels(), rotation=0, ha="right")
ax2.grid(False)
y_ticks = [2**14, 2**16, 2**18, 2**20]
y_labels = [r"$2^{14}$", r"$2^{16}$", r"$2^{18}$", r"$2^{20}$"]

ax2.set_yticks(y_ticks)
ax2.set_yticklabels(y_labels, fontsize=20 * font_ampl)

ax2.tick_params(axis="both", which="major", labelsize=20 * font_ampl)
ax2.margins(x=0.03)
ax2.spines["top"].set_visible(False)
ax2.spines["right"].set_visible(False)

# ------------------------------
# Split the legend into two parts
# ------------------------------
lines = ax2.get_lines()
labels = [line.get_label() for line in lines]

# First two in upper-left
handles1, labels1 = lines[:2], labels[:2]
font_size = 21 * font_ampl
leg1 = ax2.legend(
    handles=handles1,
    labels=labels1,
    loc='upper left',
    fontsize=font_size,
    frameon=False,
    bbox_to_anchor=(LEFT, UP),  # adjust as needed (e.g. 1.05) to raise the legend
    handletextpad=0.3,    # <<–– tighter spacing here
    labelspacing=0.1      # vertical gap

)
ax2.add_artist(leg1)

# Last two in lower-right
handles2, labels2 = lines[2:][::-1], labels[2:][::-1]
ax2.legend(
    handles=handles2,
    labels=labels2,
    loc='lower right',
    fontsize=font_size,
    frameon=False,
    bbox_to_anchor=(1.13, -0.11),  # adjust as needed (e.g. 1.05) to raise the legend
    handletextpad=0.3,    # <<–– tighter spacing here
    labelspacing=0.1      # vertical gap
)

# ==============================
# Adjust spacing and save
# ==============================
fig.subplots_adjust(
    top=0.9,
    bottom=0.19,
    left=0.102,
    right=0.98,
    wspace=0.25   # tighten between the two subplots
)

plt.savefig("combined_diagrams.svg")

