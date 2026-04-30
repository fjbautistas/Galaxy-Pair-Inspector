"""
plot_fiberflux_ratio_concept.py - Schematic of fiberflux_r / fibertotflux_r.

Usage:
    python3 pipeline/plot_fiberflux_ratio_concept.py

Outputs:
    outputs/plots/fiberflux_ratio_concept.png
    outputs/plots/fiberflux_ratio_concept.svg
"""

from pathlib import Path
import os

os.environ.setdefault("MPLCONFIGDIR", str(Path("outputs/.matplotlib").resolve()))
os.environ.setdefault("XDG_CACHE_HOME", str(Path("outputs/.cache").resolve()))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Circle
import numpy as np


OUTPUT_DIR = Path("outputs/plots")
PNG_PATH = OUTPUT_DIR / "fiberflux_ratio_concept.png"
SVG_PATH = OUTPUT_DIR / "fiberflux_ratio_concept.svg"

BG = "#ffffff"
PANEL = "#ffffff"
TEXT = "#18212b"
BLUE = "#2374ab"
ORANGE = "#d97706"
RED = "#d64550"
GREEN = "#24965a"
FIBER = "#111827"
EDGE = "#d1d9e0"

LIGHT_CMAP = LinearSegmentedColormap.from_list(
    "legacy_light",
    ["#ffffff", "#edf5ff", "#fee8a3", "#f59e42", "#c2415d", "#3b1f63"],
)


def gaussian_field(x, y, sources):
    z = np.zeros_like(x)
    for x0, y0, amp, sigma in sources:
        z += amp * np.exp(-((x - x0) ** 2 + (y - y0) ** 2) / (2 * sigma**2))
    return z


def draw_panel(ax, title, sources, fiber_centers, ratio_text, marker_labels, ratio_color):
    ax.set_facecolor(PANEL)
    ax.set_xlim(-2.7, 2.7)
    ax.set_ylim(-2.0, 2.0)
    ax.set_aspect("equal")
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_color(EDGE)
        spine.set_linewidth(1.2)

    xs = np.linspace(-2.7, 2.7, 420)
    ys = np.linspace(-2.0, 2.0, 320)
    x, y = np.meshgrid(xs, ys)
    image = gaussian_field(x, y, sources)
    ax.imshow(
        image,
        extent=[-2.7, 2.7, -2.0, 2.0],
        origin="lower",
        cmap=LIGHT_CMAP,
        alpha=0.95,
        vmin=0,
        vmax=1.25,
    )

    for fiber_center in fiber_centers:
        fiber = Circle(
            fiber_center,
            0.75,
            fill=False,
            edgecolor=FIBER,
            linewidth=2.0,
            linestyle=(0, (5, 4)),
        )
        ax.add_patch(fiber)

    for idx, (label, x0, y0) in enumerate(marker_labels):
        color = BLUE if idx == 0 else ORANGE
        ax.plot(x0, y0, "o", color=color, ms=6, mec="white", mew=0.8)
        ax.text(
            x0 + 0.13,
            y0 + 0.12,
            label,
            color=color,
            fontsize=10,
            weight="bold",
            path_effects=[pe.withStroke(linewidth=3.0, foreground="white")],
        )

    ax.text(
        0.5,
        0.96,
        title,
        transform=ax.transAxes,
        color=TEXT,
        fontsize=13.5,
        weight="bold",
        ha="center",
        va="top",
    )

    ax.text(
        0.97,
        0.08,
        ratio_text,
        transform=ax.transAxes,
        color="#ffffff",
        fontsize=13.5,
        weight="bold",
        ha="right",
        va="bottom",
        bbox=dict(boxstyle="round,pad=0.32", fc=ratio_color, ec="none", alpha=0.95),
    )


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    Path(os.environ["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)
    Path(os.environ["XDG_CACHE_HOME"]).mkdir(parents=True, exist_ok=True)

    fig = plt.figure(figsize=(14.5, 5.5), facecolor=BG)
    grid = fig.add_gridspec(
        2,
        3,
        height_ratios=[0.18, 1.0],
        hspace=0.12,
        wspace=0.10,
    )

    title_ax = fig.add_subplot(grid[0, :])
    title_ax.axis("off")
    title_ax.text(
        0.5,
        0.50,
        "Fiber Flux Ratio as a Photometric Blending Indicator",
        color=TEXT,
        fontsize=22,
        weight="bold",
        ha="center",
        va="center",
    )

    left = fig.add_subplot(grid[1, 0])
    middle = fig.add_subplot(grid[1, 1])
    right = fig.add_subplot(grid[1, 2])

    draw_panel(
        left,
        "Clean pair",
        sources=[(-0.9, 0.0, 1.15, 0.35), (1.05, 0.08, 0.85, 0.38)],
        fiber_centers=[(-0.9, 0.0), (1.05, 0.08)],
        ratio_text="P = 0.92",
        marker_labels=[("G1", -0.9, 0.0), ("G2", 1.05, 0.08)],
        ratio_color=GREEN,
    )

    draw_panel(
        middle,
        "Deblending failure",
        sources=[(-0.35, 0.02, 1.1, 0.48), (0.35, -0.03, 1.0, 0.48), (0.98, 0.25, 0.42, 0.28)],
        fiber_centers=[(-0.35, 0.02)],
        ratio_text="P = 0.48",
        marker_labels=[("G1", -0.35, 0.02), ("G2", 0.35, -0.03)],
        ratio_color=RED,
    )

    draw_panel(
        right,
        "Same-galaxy light",
        sources=[
            (-0.85, -0.08, 1.15, 0.46),
            (-0.12, 0.02, 0.78, 0.58),
            (0.82, 0.24, 0.60, 0.36),
            (1.36, 0.44, 0.34, 0.28),
            (-1.15, -0.38, 0.30, 0.34),
        ],
        fiber_centers=[(-0.85, -0.08), (0.82, 0.24)],
        ratio_text="P2 = 0.41",
        marker_labels=[("G1", -0.85, -0.08), ("G2?", 0.82, 0.24)],
        ratio_color=RED,
    )

    fig.savefig(PNG_PATH, dpi=220, bbox_inches="tight", facecolor=BG)
    fig.savefig(SVG_PATH, bbox_inches="tight", facecolor=BG)
    print(f"Guardado: {PNG_PATH}")
    print(f"Guardado: {SVG_PATH}")


if __name__ == "__main__":
    main()
