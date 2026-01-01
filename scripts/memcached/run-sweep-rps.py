#!/usr/bin/env python3
import argparse
import csv
import json
import math
import platform
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def _parse_int_list(s: str) -> List[int]:
    xs: List[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        xs.append(int(part))
    if not xs:
        raise ValueError("empty list")
    return xs


def _parse_str_list(s: str) -> List[str]:
    xs: List[str] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        xs.append(part)
    if not xs:
        raise ValueError("empty list")
    return xs


def _median(values: Sequence[float]) -> float:
    if not values:
        raise ValueError("median of empty list")
    return float(median(values))


def _git_sha(repo: Path) -> str:
    try:
        res = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return res.stdout.strip()
    except Exception:
        return "unknown"


def _sanitize_tag(tag: str) -> str:
    if "/" in tag or "\\" in tag:
        raise ValueError("tag must not contain path separators")
    if tag.strip() != tag:
        raise ValueError("tag must not have leading/trailing whitespace")
    if not tag:
        raise ValueError("tag must be non-empty")
    return tag


def _format_si(n: float) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.2f}k"
    return f"{n:.0f}"


def _nice_step(span: float, ticks: int) -> float:
    if span <= 0 or ticks <= 0:
        return 1.0
    raw = span / ticks
    exp = math.floor(math.log10(raw)) if raw > 0 else 0
    base = 10**exp
    frac = raw / base
    if frac <= 1:
        nice = 1
    elif frac <= 2:
        nice = 2
    elif frac <= 5:
        nice = 5
    else:
        nice = 10
    return nice * base


def _svg_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _write_svg_line_chart(
    out: Path,
    *,
    title: str,
    x_values: Sequence[int],
    series: Dict[str, List[Optional[float]]],
    x_label: str,
    y_label: str = "Throughput (ops/s)",
) -> None:
    width = 1100
    height = 650
    margin_left = 90
    margin_right = 320
    margin_top = 70
    margin_bottom = 90

    plot_x0 = margin_left
    plot_y0 = margin_top
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    all_y: List[float] = []
    for ys in series.values():
        all_y.extend([y for y in ys if y is not None])
    y_max = max(all_y) if all_y else 1.0
    y_max *= 1.05
    y_min = 0.0

    def x_pos(i: int) -> float:
        if len(x_values) <= 1:
            return plot_x0 + plot_w / 2
        return plot_x0 + (plot_w * i) / (len(x_values) - 1)

    def y_pos(v: float) -> float:
        if y_max <= y_min:
            return plot_y0 + plot_h
        return plot_y0 + plot_h * (1.0 - (v - y_min) / (y_max - y_min))

    palette = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
    ]
    names = list(series.keys())
    colors = {name: palette[i % len(palette)] for i, name in enumerate(names)}

    y_ticks = 6
    step = _nice_step(y_max - y_min, y_ticks)
    y_tick0 = 0.0
    y_tick_last = math.ceil(y_max / step) * step

    lines: List[str] = []
    lines.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">'
    )
    lines.append(
        '<style>'
        'text{font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial;fill:#111;}'
        '.grid{stroke:#e5e7eb;stroke-width:1;}'
        '.axis{stroke:#111;stroke-width:1.2;}'
        '.tick{stroke:#111;stroke-width:1;}'
        '</style>'
    )

    lines.append(
        f'<text x="{width/2:.1f}" y="{margin_top/2:.1f}" text-anchor="middle" '
        f'font-size="20">{_svg_escape(title)}</text>'
    )

    x_axis_y = plot_y0 + plot_h
    lines.append(
        f'<line class="axis" x1="{plot_x0}" y1="{x_axis_y}" x2="{plot_x0+plot_w}" y2="{x_axis_y}"/>'
    )
    lines.append(
        f'<line class="axis" x1="{plot_x0}" y1="{plot_y0}" x2="{plot_x0}" y2="{plot_y0+plot_h}"/>'
    )

    y = y_tick0
    while y <= y_tick_last + 1e-9:
        yp = y_pos(y)
        lines.append(
            f'<line class="grid" x1="{plot_x0}" y1="{yp:.1f}" x2="{plot_x0+plot_w}" y2="{yp:.1f}"/>'
        )
        lines.append(
            f'<line class="tick" x1="{plot_x0-6}" y1="{yp:.1f}" x2="{plot_x0}" y2="{yp:.1f}"/>'
        )
        lines.append(
            f'<text x="{plot_x0-10}" y="{yp+4:.1f}" text-anchor="end" font-size="12">{_svg_escape(_format_si(y))}</text>'
        )
        y += step

    for i, x in enumerate(x_values):
        xp = x_pos(i)
        lines.append(
            f'<line class="tick" x1="{xp:.1f}" y1="{x_axis_y}" x2="{xp:.1f}" y2="{x_axis_y+6}"/>'
        )
        lines.append(
            f'<text x="{xp:.1f}" y="{x_axis_y+24}" text-anchor="middle" font-size="12">{x}</text>'
        )

    lines.append(
        f'<text x="{plot_x0+plot_w/2:.1f}" y="{height-30}" text-anchor="middle" font-size="14">{_svg_escape(x_label)}</text>'
    )
    lines.append(
        f'<text x="20" y="{plot_y0+plot_h/2:.1f}" text-anchor="middle" font-size="14" transform="rotate(-90 20 {plot_y0+plot_h/2:.1f})">'
        f"{_svg_escape(y_label)}</text>"
    )

    for name, ys in series.items():
        pts: List[Tuple[float, float]] = []
        for i, yv in enumerate(ys):
            if yv is None:
                continue
            pts.append((x_pos(i), y_pos(yv)))
        if len(pts) >= 2:
            poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
            lines.append(
                f'<polyline fill="none" stroke="{colors[name]}" stroke-width="2.4" points="{poly}"/>'
            )
        for x, y in pts:
            lines.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.0" fill="{colors[name]}" stroke="#fff" stroke-width="1"/>'
            )

    legend_x0 = plot_x0 + plot_w + 20
    legend_y0 = plot_y0 + 10
    lines.append(
        f'<text x="{legend_x0}" y="{legend_y0-8}" font-size="14" font-weight="600">series</text>'
    )
    for i, name in enumerate(names):
        y = legend_y0 + i * 22
        lines.append(
            f'<rect x="{legend_x0}" y="{y-10}" width="14" height="14" fill="{colors[name]}"/>'
        )
        lines.append(
            f'<text x="{legend_x0+20}" y="{y+2}" font-size="12">{_svg_escape(name)}</text>'
        )

    lines.append("</svg>")
    out.write_text("\n".join(lines) + "\n", encoding="utf8")


@dataclass(frozen=True)
class RunResult:
    rps: int
    sei_variant: str
    repeat: int
    tag: str
    throughput_json: str
    vanilla: float
    sei: float
    orthrus: float
    orthrus_sync: Optional[float]
    rbv: float


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run memcached sweep over --rps (rate limiting for UPDATE/GET) and generate a throughput plot."
    )
    parser.add_argument("--build-dir", default="build")
    parser.add_argument("--preset", choices=["default", "fair4c"], default="fair4c")
    parser.add_argument(
        "--server-ip",
        default="127.0.0.1",
        help="IPv4 address to pass to memcached_client (default: 127.0.0.1).",
    )
    parser.add_argument("--port-start", type=int, default=20000)
    parser.add_argument("--port-end", type=int, default=40000)
    parser.add_argument(
        "--client-ssh",
        default=None,
        help="If set, run the client on a remote load host via SSH (example: user@loadhost).",
    )
    parser.add_argument("--client-workdir", default=None)
    parser.add_argument("--remote-client-bin", default=None)
    parser.add_argument("--client-temp-dir", default="/tmp/orthrus-memcached")
    parser.add_argument("--client-pin-cpus", default=None)
    parser.add_argument(
        "--rps",
        default="0,1000,2000,4000,8000,12000,16000",
        help="Comma-separated list of rps values passed to client (default: 0,1000,2000,4000,8000,12000,16000).",
    )
    parser.add_argument("--nclients", type=int, default=16)
    parser.add_argument(
        "--sei-variants",
        default="er2,er5,er10,dynamicNway,dynamicCore",
        help="Comma-separated SEI variants to sweep (default: er2,er5,er10,dynamicNway,dynamicCore).",
    )
    parser.add_argument(
        "--orthrus-sync",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Also run Orthrus with synchronous validation (SCEE_SYNC_VALIDATE).",
    )
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--pin", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--mode",
        choices=["throughput", "memory", "all"],
        default="throughput",
    )
    parser.add_argument("--ngroups", type=int, default=None)
    parser.add_argument("--vanilla-ngroups", type=int, default=None)
    parser.add_argument("--sei-ngroups", type=int, default=None)
    parser.add_argument("--orthrus-ngroups", type=int, default=None)
    parser.add_argument("--rbv-ngroups", type=int, default=None)
    parser.add_argument("--nsets-exp", type=int, default=None)
    parser.add_argument("--ngets-exp", type=int, default=None)
    parser.add_argument("--timeout-sec", type=int, default=None)
    parser.add_argument(
        "--tag-prefix",
        default=None,
        help="Prefix used for per-run tags (default: sweep_rps.<preset>.<timestamp>).",
    )
    parser.add_argument(
        "--out-tag",
        default=None,
        help="Tag used for aggregated outputs (default: <tag-prefix>).",
    )
    parser.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip runs if their throughput JSON already exists (default: enabled).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="If set, re-run even if output files already exist.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing.",
    )
    args = parser.parse_args()

    if args.nclients <= 0:
        raise ValueError("--nclients must be >= 1")
    if args.repeats <= 0:
        raise ValueError("--repeats must be >= 1")

    rps_list = _parse_int_list(args.rps)
    sei_variants = _parse_str_list(args.sei_variants)
    for rps in rps_list:
        if rps < 0:
            raise ValueError("rps values must be >= 0")

    root = Path(__file__).resolve().parents[2]
    results_dir = root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    run_compare = root / "scripts" / "memcached" / "run-compare.py"

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    tag_prefix = args.tag_prefix or f"sweep_rps.{args.preset}.{timestamp}"
    tag_prefix = _sanitize_tag(tag_prefix)
    out_tag = _sanitize_tag(args.out_tag or tag_prefix)

    sei_series_name = {
        "default": "sei_default",
        "er2": "sei_er2",
        "er5": "sei_er5",
        "er10": "sei_er10",
        "dynamicNway": "sei_dynamicNway_rb_er5",
        "dynamicCore": "sei_dynamicCore_rb",
    }

    runs: List[RunResult] = []
    total = len(rps_list) * len(sei_variants) * args.repeats
    done = 0
    for rps in rps_list:
        for variant in sei_variants:
            if variant not in sei_series_name:
                raise ValueError(f"Unknown sei variant: {variant}")
            for r in range(1, args.repeats + 1):
                tag = _sanitize_tag(f"{tag_prefix}.rps{rps}.sei{variant}.r{r}")
                out_json = results_dir / f"memcached-throughput-report.{tag}.txt.json"
                if not args.force and args.resume and out_json.exists():
                    with open(out_json, encoding="utf8") as f:
                        data = json.load(f)
                    if not (args.orthrus_sync and "orthrus_sync" not in data):
                        if args.dry_run:
                            print(
                                f"[dry-run] would load existing {out_json}",
                                file=sys.stderr,
                            )
                        else:
                            runs.append(
                                RunResult(
                                    rps=rps,
                                    sei_variant=variant,
                                    repeat=r,
                                    tag=tag,
                                    throughput_json=str(out_json.relative_to(root)),
                                    vanilla=float(data["vanilla"]["throughput"]),
                                    sei=float(data["sei"]["throughput"]),
                                    orthrus=float(data["orthrus"]["throughput"]),
                                    orthrus_sync=(
                                        float(data["orthrus_sync"]["throughput"])
                                        if "orthrus_sync" in data
                                        else None
                                    ),
                                    rbv=float(data["rbv"]["throughput"]),
                                )
                            )
                        done += 1
                        continue

                cmd: List[str] = [
                    sys.executable,
                    str(run_compare),
                    "--build-dir",
                    str(args.build_dir),
                    "--preset",
                    args.preset,
                    "--server-ip",
                    args.server_ip,
                    "--port-start",
                    str(args.port_start),
                    "--port-end",
                    str(args.port_end),
                    "--sei-variant",
                    variant,
                    "--nclients",
                    str(args.nclients),
                    "--rps",
                    str(rps),
                    "--mode",
                    args.mode,
                    "--tag",
                    tag,
                ]
                if not args.pin:
                    cmd.append("--no-pin")
                if args.ngroups is not None:
                    cmd += ["--ngroups", str(args.ngroups)]
                if args.vanilla_ngroups is not None:
                    cmd += ["--vanilla-ngroups", str(args.vanilla_ngroups)]
                if args.sei_ngroups is not None:
                    cmd += ["--sei-ngroups", str(args.sei_ngroups)]
                if args.orthrus_ngroups is not None:
                    cmd += ["--orthrus-ngroups", str(args.orthrus_ngroups)]
                if args.rbv_ngroups is not None:
                    cmd += ["--rbv-ngroups", str(args.rbv_ngroups)]
                if args.orthrus_sync:
                    cmd.append("--orthrus-sync")
                if args.nsets_exp is not None:
                    cmd += ["--nsets-exp", str(args.nsets_exp)]
                if args.ngets_exp is not None:
                    cmd += ["--ngets-exp", str(args.ngets_exp)]
                if args.timeout_sec is not None:
                    cmd += ["--timeout-sec", str(args.timeout_sec)]
                if args.client_ssh is not None:
                    cmd += ["--client-ssh", args.client_ssh]
                    if args.client_workdir is not None:
                        cmd += ["--client-workdir", args.client_workdir]
                    if args.remote_client_bin is not None:
                        cmd += ["--remote-client-bin", args.remote_client_bin]
                    if args.client_temp_dir is not None:
                        cmd += ["--client-temp-dir", args.client_temp_dir]
                    if args.client_pin_cpus is not None:
                        cmd += ["--client-pin-cpus", args.client_pin_cpus]

                done += 1
                print(
                    f"[{done}/{total}] rps={rps} nclients={args.nclients} sei_variant={variant} r={r}",
                    file=sys.stderr,
                )
                print("+", " ".join(cmd), file=sys.stderr)
                if args.dry_run:
                    continue
                subprocess.run(cmd, cwd=str(root), check=True)

                if not out_json.exists():
                    raise FileNotFoundError(str(out_json))
                with open(out_json, encoding="utf8") as f:
                    data = json.load(f)
                runs.append(
                    RunResult(
                        rps=rps,
                        sei_variant=variant,
                        repeat=r,
                        tag=tag,
                        throughput_json=str(out_json.relative_to(root)),
                        vanilla=float(data["vanilla"]["throughput"]),
                        sei=float(data["sei"]["throughput"]),
                        orthrus=float(data["orthrus"]["throughput"]),
                        orthrus_sync=(
                            float(data["orthrus_sync"]["throughput"])
                            if "orthrus_sync" in data
                            else None
                        ),
                        rbv=float(data["rbv"]["throughput"]),
                    )
                )

    if args.dry_run:
        return 0

    def pick(values: Iterable[RunResult], key: str) -> List[float]:
        out: List[float] = []
        for rr in values:
            out.append(getattr(rr, key))
        return out

    def pick_optional(values: Iterable[RunResult], key: str) -> List[float]:
        out: List[float] = []
        for rr in values:
            v = getattr(rr, key)
            if v is None:
                continue
            out.append(v)
        return out

    by_rps: Dict[int, List[RunResult]] = {}
    for rr in runs:
        by_rps.setdefault(rr.rps, []).append(rr)

    base_series: Dict[str, List[Optional[float]]] = {
        "vanilla": [],
        "orthrus": [],
        **({"orthrus_sync": []} if args.orthrus_sync else {}),
        "rbv": [],
    }
    for rps in rps_list:
        vals = by_rps.get(rps, [])
        if not vals:
            for k in base_series:
                base_series[k].append(None)
            continue
        base_series["vanilla"].append(_median(pick(vals, "vanilla")))
        base_series["orthrus"].append(_median(pick(vals, "orthrus")))
        if args.orthrus_sync:
            sync_vals = pick_optional(vals, "orthrus_sync")
            base_series["orthrus_sync"].append(_median(sync_vals) if sync_vals else None)
        base_series["rbv"].append(_median(pick(vals, "rbv")))

    sei_series: Dict[str, List[Optional[float]]] = {}
    for variant in sei_variants:
        name = sei_series_name[variant]
        ys: List[Optional[float]] = []
        for rps in rps_list:
            vals = [rr.sei for rr in by_rps.get(rps, []) if rr.sei_variant == variant]
            ys.append(_median(vals) if vals else None)
        sei_series[name] = ys

    all_series: Dict[str, List[Optional[float]]] = {**base_series, **sei_series}

    out_json = results_dir / f"memcached-throughput-vs-rps.{out_tag}.json"
    out_csv = results_dir / f"memcached-throughput-vs-rps.{out_tag}.csv"
    out_svg = results_dir / f"memcached-throughput-vs-rps.{out_tag}.svg"

    orthus_sha = _git_sha(root)
    libsei_sha = _git_sha((root / ".." / "libsei-gcc").resolve())

    out_json.write_text(
        json.dumps(
            {
                "preset": args.preset,
                "nclients": args.nclients,
                "rps": rps_list,
                "sei_variants": sei_variants,
                "orthrus_sync": args.orthrus_sync,
                "repeats": args.repeats,
                "mode": args.mode,
                "build_dir": args.build_dir,
                "pin": args.pin,
                "server_ip": args.server_ip,
                "port_range": {"start": args.port_start, "end": args.port_end},
                "client_ssh": args.client_ssh,
                "client_workdir": args.client_workdir,
                "remote_client_bin": args.remote_client_bin,
                "client_temp_dir": args.client_temp_dir if args.client_ssh else None,
                "client_pin_cpus": args.client_pin_cpus,
                "meta": {
                    "timestamp": timestamp,
                    "host": platform.node(),
                    "uname": " ".join(platform.uname()),
                    "python": sys.version,
                    "sha": {"Orthrus": orthus_sha, "libsei-gcc": libsei_sha},
                },
                "note": {
                    "rps_semantics": "rps is passed to the client and applies to UPDATE/GET only; internally the client computes rps_per_thread=rps*ngroups/nclients",
                    "rps_0": "rps=0 means no rate limiting (max load).",
                },
                "runs": [rr.__dict__ for rr in runs],
                "series": all_series,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf8",
    )

    series_names = ["vanilla", *sei_series.keys(), "orthrus"]
    if args.orthrus_sync:
        series_names.append("orthrus_sync")
    series_names.append("rbv")
    with open(out_csv, "w", encoding="utf8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["rps", *series_names])
        for i, rps in enumerate(rps_list):
            row: List[object] = [rps]
            for name in series_names:
                v = all_series.get(name, [None] * len(rps_list))[i]
                row.append("" if v is None else f"{v:.3f}")
            w.writerow(row)

    plot_series = {name: all_series[name] for name in series_names if name in all_series}
    _write_svg_line_chart(
        out_svg,
        title=f"memcached throughput vs rps (preset={args.preset}, nclients={args.nclients})",
        x_values=rps_list,
        series=plot_series,
        x_label="rps (client arg; UPDATE/GET only)",
    )

    print(f"Wrote {out_json}", file=sys.stderr)
    print(f"Wrote {out_csv}", file=sys.stderr)
    print(f"Wrote {out_svg}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
