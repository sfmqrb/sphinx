import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from consts import amp2, proj_name

# ── 0) Theme ───────────────────────────────────────────────────────────────
sns.set_theme(
    style="whitegrid",
    rc={
        "font.family": "serif",
        "font.serif": ["Times New Roman"],
        "font.size": 20,
        "text.usetex": False,
        "mathtext.fontset": "custom",
        "mathtext.rm": "Times New Roman",
    },
)

font_amp = 1.3
# ── 1) Load data ───────────────────────────────────────────────────────────
home_dir = os.environ["HOME"]
base = os.path.join(home_dir, "research", proj_name, "benchmark", "data-memory")
df_xdp = pd.read_csv(os.path.join(base, "benchmark_XDP.csv"), index_col="num_entries")
df_sphinx = pd.read_csv(
    os.path.join(base, "benchmark_Sphinx.csv"), index_col="num_entries"
)

# ── 2) Compute closest indices ──────────────────────────────────────────────
targets = [10**6, 10**7, 10**8]
entries = df_xdp.index.to_numpy()
closest_exact = [int(entries[np.abs(entries - T).argmin()]) for T in targets]

# ── 3) Rolling-min(25) ──────────────────────────────────────────────────────
df_xdp_rm = df_xdp.rolling(25, min_periods=1).min()
df_sphinx_rm = df_sphinx.rolling(25, min_periods=1).min()
xdp_sel = df_xdp_rm.loc[closest_exact]
sphinx_sel = df_sphinx_rm.loc[closest_exact].copy()

# ── 4) Compute Sphinx overhead ───────────────────────────────────────────────
sphinx_sel["overhead"] = sphinx_sel["memory_including_ptr"] - sphinx_sel["memory"]

# ── 5) Segments & palettes ──────────────────────────────────────────────────
xdp_segs = ["index_global", "index_local", "ptr_global", "ptr_local"]
sphinx_segs = ["memory", "overhead"]
greens = ["#1b7837", "#4dac26"]  # even darker greens for indexes
grays = ["#a0a0a0", "#4e4e4e"]  # darker grays for pointers
xdp_colors = greens + grays
xdp_hatches = ["//", "\\\\", "..", "xx"]  # tighter hatches
sphinx_colors = ["#00441b", "#000000"]  # dark green and black for sphinx
sphinx_hatches = ["--", "o"]

# ── 6) Bar positions ────────────────────────────────────────────────────────
x = np.arange(len(closest_exact))
width = 0.8
alpha = 1.85
xdp_pos = alpha * x - width / 2
sphinx_pos = alpha * x + width / 2

# ── 7) Plot ─────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(11, 4.3))

# XDP bars
bottom = np.zeros(len(x))
legend_map = {
    "index_global": "global metadata",
    "index_local": "local metadata",
    "ptr_global": "global pointer",
    "ptr_local": "local pointer",
}
for i, seg in enumerate(xdp_segs):
    col = xdp_colors[i]
    ax.bar(
        xdp_pos,
        xdp_sel[seg].values,
        width,
        bottom=bottom,
        facecolor="white",
        edgecolor=col,
        hatch=xdp_hatches[i],
        linewidth=1.5,
        label=legend_map[seg],
    )
    # Matplotlib>=3.4 supports hatch_color:
    for bar in ax.containers[-1]:
        bar.set_hatch(bar.get_hatch())
        bar.set_edgecolor(col)
        bar.set_facecolor("white")
        bar.set_linewidth(1.5)
    bottom += xdp_sel[seg].values

# Sphinx bars
bottom = np.zeros(len(x))
sphinx_map = {"memory": "normal metadata", "overhead": "normal pointer"}
for i, seg in enumerate(sphinx_segs):
    col = sphinx_colors[i]
    ax.bar(
        sphinx_pos,
        sphinx_sel[seg].values,
        width,
        bottom=bottom,
        facecolor="white",
        edgecolor=col,
        hatch=sphinx_hatches[i],
        linewidth=1.5,
        label=sphinx_map[seg],
    )
    for bar in ax.containers[-1]:
        bar.set_hatch(bar.get_hatch())
        bar.set_edgecolor(col)
        bar.set_facecolor("white")
        bar.set_linewidth(1.5)
    bottom += sphinx_sel[seg].values

# ── 8) X-axis labeling ──────────────────────────────────────────────────────
diff = 0.06
all_positions = np.ravel(
    [[xp - diff, sp + diff] for xp, sp in zip(xdp_pos, sphinx_pos)]
)
ax.set_xticks(all_positions)
ax.set_yticks(np.arange(0, 43, 10))
ax.set_xticklabels(["compact", "normal"] * len(closest_exact))
# use 10**6, 10**7, 10**8 as labels
for xi, lab in zip(x - 0.08, [r"$10^6$", r"$10^7$", r"$10^8$"]):
    ax.text(
        xi * alpha,
        -0.13,
        lab,
        transform=ax.get_xaxis_transform(),
        ha="center",
        va="top",
        fontsize=20 * font_amp,
    )

ax.set_xlabel("#entries", fontsize=22 * font_amp)
ax.xaxis.set_label_coords(0.46, -0.25)

# ── 9) Styling ───────────────────────────────────────────────────────────────
ax.set_ylabel("min memory (bits/entry)", fontsize=22 * font_amp, fontweight="bold")
ax.yaxis.set_label_coords(-0.08, 0.35)
ax.tick_params(axis="y", labelsize=20 * font_amp)
ax.tick_params(axis="x", labelsize=18.5 * font_amp, rotation=15, pad=-8)

ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.grid(False)

# ── 10) Legend ──────────────────────────────────────────────────────────────
handles, labels = ax.get_legend_handles_labels()
order = [
    "global metadata",
    "local metadata",
    "normal metadata",
    "global pointer",
    "local pointer",
    "normal pointer",
]
new_handles = [handles[labels.index(name)] for name in order]

handles1, labels1 = new_handles[:3], order[:3]
leg1 = ax.legend(
    handles1,
    labels1,
    title="",
    fontsize=21 * font_amp,
    loc="center left",
    bbox_to_anchor=(0.965, 0.15),
    frameon=False,
    handletextpad=0.3,  # <<–– tighter spacing here
    # labelspacing=0.1      # vertical gap
)

ax.add_artist(leg1)

handles2, labels2 = new_handles[3:], order[3:]
ax.legend(
    handles2,
    labels2,
    title="",
    fontsize=21 * font_amp,
    loc="center left",
    bbox_to_anchor=(0.965, 0.72),
    frameon=False,
    handletextpad=0.3,  # <<–– tighter spacing here
    # labelspacing=0.1      # vertical gap
)
ax.margins(x=0.01)


# ── 11) Tighten margins ─────────────────────────────────────────────────────
plt.tight_layout(pad=0.1)
plt.subplots_adjust(top=0.98, bottom=0.25, left=0.082, right=0.680)

plt.savefig("memory_comparison.svg", dpi=300)
plt.close(fig)
