import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from consts import amp2, proj_name

normal_ptr_label = "pointer"
global_ptr = "chunk ID"
local_ptr = "page offset"
index_meta = "Trie Store"


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
closest_exact1 = [1.05587e+06, 8.44696e+06, 6.75756e+07]
closest_exact2 = [992014, 7.93611e+06, 6.34889e+07]

# ── 3) Rolling-min(25) ──────────────────────────────────────────────────────
df_xdp_rm = df_xdp.rolling(1, min_periods=1).min()
df_sphinx_rm = df_sphinx.rolling(1, min_periods=1).min()
xdp_sel = df_xdp_rm.loc[closest_exact1]
sphinx_sel = df_sphinx_rm.loc[closest_exact2].copy()

# ── 4) Compute Sphinx overhead ───────────────────────────────────────────────
sphinx_sel["overhead"] = sphinx_sel["memory_including_ptr"] - sphinx_sel["memory"]

# ── 5) Compute merged metadata and pointers ─────────────────────────────────
xdp_meta = xdp_sel["index_global"] + xdp_sel["index_local"]
xdp_global_ptr = xdp_sel["ptr_global"]
xdp_local_ptr = xdp_sel["ptr_local"]
sphinx_meta = sphinx_sel["memory"]
normal_ptr = sphinx_sel["overhead"]

# ── 6) Bar positions ────────────────────────────────────────────────────────
x = np.arange(len(closest_exact1))
width = 0.8
alpha = 1.85
xdp_pos = alpha * x + width / 2
sphinx_pos = alpha * x - width / 2

# ── 7) Plot ─────────────────────────────────────────────────────────────────
fig, ax = plt.subplots(figsize=(11, 4.3))

# Define hatches (black-and-white only)
hatch_meta = '////'
hatch_global = '\\'
hatch_local = '\\\\'
hatch_normal = '//'
# XDP bars (compact)
bottom = np.zeros(len(x))
ax.bar(
    xdp_pos,
    xdp_meta.values,
    width,
    bottom=bottom,
    facecolor='white',
    edgecolor='black',
    hatch=hatch_meta,
    linewidth=1.5,
    label=index_meta,
)
bottom += xdp_meta.values
ax.bar(
    xdp_pos,
    xdp_global_ptr.values,
    width,
    bottom=bottom,
    facecolor='white',
    edgecolor='black',
    hatch=hatch_global,
    linewidth=1.5,
    label=global_ptr,
)
bottom += xdp_global_ptr.values
ax.bar(
    xdp_pos,
    xdp_local_ptr.values,
    width,
    bottom=bottom,
    facecolor='white',
    edgecolor='black',
    hatch=hatch_local,
    linewidth=1.5,
    label=local_ptr,
)

# Sphinx bars (normal)
bottom = np.zeros(len(x))
ax.bar(
    sphinx_pos,
    sphinx_meta.values,
    width,
    bottom=bottom,
    facecolor='white',
    edgecolor='black',
    hatch=hatch_meta,
    linewidth=1.5,
    label=index_meta,
)
bottom += sphinx_meta.values
ax.bar(
    sphinx_pos,
    normal_ptr.values,
    width,
    bottom=bottom,
    facecolor='white',
    edgecolor='black',
    hatch=hatch_normal,
    linewidth=1.5,
    label=normal_ptr_label,
)

# ── 8) X-axis labeling ──────────────────────────────────────────────────────
diff = 0.06
all_positions = np.ravel(
    [[xp - diff, sp + diff] for xp, sp in zip(xdp_pos, sphinx_pos)]
)
ax.set_xticks(all_positions)
ax.set_yticks(np.arange(0, 43, 10))
ax.set_xticklabels(["compres.", "full"] * len(closest_exact1))
# use 10**6, 10**7, 10**8 as labels
for xi, lab in zip(x, [r"$10^6$", r"$10^7$", r"$10^8$"]):
    ax.text(
        xi * alpha,
        -0.2,
        lab,
        transform=ax.get_xaxis_transform(),
        ha="center",
        va="top",
        fontsize=20 * font_amp,
    )

ax.set_xlabel("#entries", fontsize=22 * font_amp)
ax.xaxis.set_label_coords(0.49, -0.41)

# ── 9) Styling ───────────────────────────────────────────────────────────────
ax.set_ylabel("bits / entry", fontsize=22 * font_amp, fontweight="bold")
ax.yaxis.set_label_coords(-0.055, 0.5)
ax.tick_params(axis="y", labelsize=20 * font_amp)
ax.tick_params(axis="x", labelsize=20 * font_amp, rotation=0, pad=-1)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.grid(False)

# ── 10) Legend ──────────────────────────────────────────────────────────────
handles, labels = ax.get_legend_handles_labels()
order = [index_meta, global_ptr, local_ptr, normal_ptr_label]
print(order)
print("-----------------------------")
new_handles = [handles[labels.index(name)] for name in order]
ax.legend(
    new_handles,
    order,
    title="",
    fontsize=21 * font_amp,
    loc="upper center",
    bbox_to_anchor=(0.45, 1.37),
    frameon=False,
    handletextpad=0.3,
    ncol=4,
    columnspacing=0.5,
    handlelength=2.5,
)
ax.margins(x=0.01)

# ── 11) Tighten margins ─────────────────────────────────────────────────────
plt.tight_layout(pad=0.1)
plt.subplots_adjust(top=0.87, bottom=0.30, left=0.082, right=1)

plt.savefig("memory_comparison.svg", dpi=300)
plt.close(fig)

