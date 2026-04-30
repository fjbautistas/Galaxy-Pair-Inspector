"""
plot_blending_deblending_concept.py - Visual schematic of blending/deblending.

Usage:
    python3 pipeline/plot_blending_deblending_concept.py

Outputs:
    outputs/plots/blending_deblending_concept.png
    outputs/plots/blending_deblending_concept.svg
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
from matplotlib.patches import Ellipse, FancyArrowPatch
import numpy as np


OUTPUT_DIR = Path("outputs/plots")
PNG_PATH = OUTPUT_DIR / "blending_deblending_concept.png"
SVG_PATH = OUTPUT_DIR / "blending_deblending_concept.svg"

BG = "#ffffff"
TEXT = "#18212b"
EDGE = "#d1d9e0"
BLUE = "#2374ab"
ORANGE = "#d97706"
RED = "#d64550"

LIGHT_CMAP = LinearSegmentedColormap.from_list(
    "legacy_light",
    ["#ffffff", "#edf5ff", "#fee8a3", "#f59e42", "#c2415d", "#3b1f63"],
)


def gaussian_field(x, y, sources):
    z = np.zeros_like(x)
    for x0, y0, amp, sx, sy, theta in sources:
        ct, st = np.cos(theta), np.sin(theta)
        xr = (x - x0) * ct + (y - y0) * st
        yr = -(x - x0) * st + (y - y0) * ct
        z += amp * np.exp(-(xr**2 / (2 * sx**2) + yr**2 / (2 * sy**2)))
    return z


def draw_image(ax, title, sources, markers, contours=None, subtitle=None):
    ax.set_facecolor(BG)
    ax.set_xlim(-2.6, 2.6)
    ax.set_ylim(-1.8, 1.8)
    ax.set_aspect("equal")
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_color(EDGE)
        spine.set_linewidth(1.2)

    xs = np.linspace(-2.6, 2.6, 440)
    ys = np.linspace(-1.8, 1.8, 320)
    x, y = np.meshgrid(xs, ys)
    image = gaussian_field(x, y, sources)
    ax.imshow(
        image,
        extent=[-2.6, 2.6, -1.8, 1.8],
        origin="lower",
        cmap=LIGHT_CMAP,
        alpha=0.96,
        vmin=0,
        vmax=1.35,
    )

    if contours:
        for x0, y0, w, h, angle, color, style in contours:
            ax.add_patch(
                Ellipse(
                    (x0, y0),
                    w,
                    h,
                    angle=angle,
                    fill=False,
                    edgecolor=color,
                    linewidth=2.2,
                    linestyle=style,
                    alpha=0.95,
                )
            )

    for label, x0, y0, color in markers:
        ax.plot(x0, y0, "o", ms=6, color=color, mec="white", mew=0.9)
        ax.text(
            x0 + 0.12,
            y0 + 0.10,
            label,
            color=color,
            fontsize=10.5,
            weight="bold",
            path_effects=[pe.withStroke(linewidth=3.0, foreground="white")],
        )

    ax.text(
        0.5,
        0.95,
        title,
        transform=ax.transAxes,
        ha="center",
        va="top",
        color=TEXT,
        fontsize=14,
        weight="bold",
    )
    if subtitle:
        ax.text(
            0.5,
            0.08,
            subtitle,
            transform=ax.transAxes,
            ha="center",
            va="bottom",
            color="#ffffff",
            fontsize=11,
            weight="bold",
            bbox=dict(boxstyle="round,pad=0.30", fc=RED, ec="none", alpha=0.95),
        )


def add_arrow(fig, left_ax, right_ax):
    left = left_ax.get_position()
    right = right_ax.get_position()
    y = (left.y0 + left.y1) / 2
    arrow = FancyArrowPatch(
        (left.x1 + 0.012, y),
        (right.x0 - 0.012, y),
        transform=fig.transFigure,
        arrowstyle="-|>",
        mutation_scale=20,
        linewidth=2,
        color="#8a98a8",
    )
    fig.patches.append(arrow)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    Path(os.environ["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)
    Path(os.environ["XDG_CACHE_HOME"]).mkdir(parents=True, exist_ok=True)

    fig = plt.figure(figsize=(14.5, 5.3), facecolor=BG)
    grid = fig.add_gridspec(2, 3, height_ratios=[0.18, 1.0], hspace=0.12, wspace=0.12)

    title_ax = fig.add_subplot(grid[0, :])
    title_ax.axis("off")
    title_ax.text(
        0.5,
        0.50,
        "Blending and Deblending in Galaxy Images",
        color=TEXT,
        fontsize=22,
        weight="bold",
        ha="center",
        va="center",
    )

    separated = fig.add_subplot(grid[1, 0])
    blended = fig.add_subplot(grid[1, 1])
    deblended = fig.add_subplot(grid[1, 2])

    g1 = (-0.95, -0.05, 1.15, 0.40, 0.28, np.deg2rad(18))
    g2_separate = (0.95, 0.05, 0.95, 0.38, 0.25, np.deg2rad(-12))
    g2_close = (0.10, 0.02, 0.95, 0.42, 0.28, np.deg2rad(-12))

    draw_image(
        separated,
        "Two sources",
        [g1, g2_separate],
        [("G1", -0.95, -0.05, BLUE), ("G2", 0.95, 0.05, ORANGE)],
        contours=[
            (-0.95, -0.05, 1.30, 0.88, 18, BLUE, "-"),
            (0.95, 0.05, 1.20, 0.80, -12, ORANGE, "-"),
        ],
    )

    draw_image(
        blended,
        "Blending",
        [g1, g2_close],
        [("G1", -0.95, -0.05, BLUE), ("G2", 0.10, 0.02, ORANGE)],
        contours=[(-0.42, -0.02, 2.25, 1.05, 2, TEXT, (0, (5, 4)))],
    )

    draw_image(
        deblended,
        "Deblending",
        [g1, g2_close],
        [("G1", -0.95, -0.05, BLUE), ("G2?", 0.10, 0.02, ORANGE)],
        contours=[
            (-0.82, -0.04, 1.35, 0.95, 18, BLUE, "-"),
            (0.06, 0.03, 1.10, 0.78, -12, ORANGE, "-"),
            (-0.30, -0.02, 2.15, 1.08, 2, RED, (0, (5, 4))),
        ],
        subtitle="model split",
    )

    add_arrow(fig, separated, blended)
    add_arrow(fig, blended, deblended)

    fig.savefig(PNG_PATH, dpi=220, bbox_inches="tight", facecolor=BG)
    fig.savefig(SVG_PATH, bbox_inches="tight", facecolor=BG)
    print(f"Guardado: {PNG_PATH}")
    print(f"Guardado: {SVG_PATH}")


if __name__ == "__main__":
    main()
