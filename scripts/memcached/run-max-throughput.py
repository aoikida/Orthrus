#!/usr/bin/env python3
import argparse
import csv
import json
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from statistics import median
from typing import Dict, List, Optional, Sequence

SEI_VARIANT_CHOICES = ["er2", "er5", "er10", "dynamicNway", "core", "dynamicCore"]
SEI_VARIANT_ALIASES = {"default": "er2"}


def _normalize_sei_variant(variant: str) -> str:
    v = variant.strip()
    return SEI_VARIANT_ALIASES.get(v, v)


def _parse_sei_variants(s: str) -> List[str]:
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if not parts:
        raise ValueError("empty list")
    out: List[str] = []
    seen = set()
    for p in parts:
        p = _normalize_sei_variant(p)
        if p not in SEI_VARIANT_CHOICES:
            raise ValueError(f"Unknown sei variant: {p}")
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


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


@dataclass(frozen=True)
class RunResult:
    repeat: int
    tag: str
    throughput_json: str
    vanilla: float
    sei: Dict[str, float]
    orthrus: float
    orthrus_sync: Optional[float]
    rbv: float
    rbv_sync: Optional[float]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Measure max throughput for each variant by running run-compare.py with "
            "--rps-per-thread 0 (i.e., rps=0) and aggregating results."
        )
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
    parser.add_argument("--nclients", type=int, default=16)
    parser.add_argument(
        "--read-pct",
        type=float,
        default=None,
        help=(
            "If set, configure memcached_client to be read-heavy: percentage of "
            "GETs among (UPDATE+GET) after initial SET (example: 95 for ~95%% reads)."
        ),
    )
    parser.add_argument(
        "--sei-variants",
        default="er2,er5,er10,dynamicNway,core,dynamicCore",
        help="Comma-separated SEI variants to run (default: er2,er5,er10,dynamicNway,core,dynamicCore).",
    )
    parser.add_argument(
        "--orthrus-sync",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also run Orthrus with synchronous validation (SCEE_SYNC_VALIDATE) (default: enabled).",
    )
    parser.add_argument(
        "--rbv-sync",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also run RBV with synchronous validation (rbv_sync series) (default: enabled).",
    )
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--pin", action=argparse.BooleanOptionalAction, default=True)
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
        help="Prefix used for per-run tags (default: max_tp.<preset>.ncl<N>.<timestamp>).",
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
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.port_start <= 0 or args.port_end <= 0:
        raise ValueError("--port-start/--port-end must be > 0")
    if args.port_end <= args.port_start:
        raise ValueError("--port-end must be > --port-start")
    if args.repeats <= 0:
        raise ValueError("--repeats must be >= 1")
    if args.nclients <= 0:
        raise ValueError("--nclients must be >= 1")

    sei_variants = _parse_sei_variants(args.sei_variants)

    if args.read_pct is not None:
        read_pct = float(args.read_pct)
        if read_pct <= 1.0:
            read_pct *= 100.0
        if not (read_pct > 0.0 and read_pct <= 100.0):
            raise ValueError("--read-pct must be in (0,100] (or provide a ratio in (0,1]).")
        args.read_pct = read_pct

    root = Path(__file__).resolve().parents[2]
    results_dir = root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    run_compare = root / "scripts" / "memcached" / "run-compare.py"

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    tag_prefix = args.tag_prefix or f"max_tp.{args.preset}.ncl{args.nclients}.{timestamp}"
    tag_prefix = _sanitize_tag(tag_prefix)
    out_tag = _sanitize_tag(args.out_tag or tag_prefix)

    sei_series_name = {
        "er2": "sei_er2",
        "er5": "sei_er5",
        "er10": "sei_er10",
        "dynamicNway": "sei_dynamicNway_rb_er5",
        "core": "sei_core",
        "dynamicCore": "sei_dynamicCore_rb",
    }

    def _extract_sei_throughputs(data: dict) -> Dict[str, float]:
        blob = data.get("sei")
        if not isinstance(blob, dict):
            raise ValueError("invalid throughput json: missing 'sei' dict")
        if "throughput" in blob:
            if len(sei_variants) != 1:
                raise ValueError(
                    "existing throughput json contains only one SEI result; rerun without --resume"
                )
            return {sei_variants[0]: float(blob["throughput"])}

        out: Dict[str, float] = {}
        for v in sei_variants:
            v_obj = blob.get(v)
            if v_obj is None and v == "er2":
                v_obj = blob.get("default")
            if not isinstance(v_obj, dict) or "throughput" not in v_obj:
                raise ValueError(f"missing SEI variant result in json: {v}")
            out[v] = float(v_obj["throughput"])
        return out

    runs: List[RunResult] = []
    for r in range(1, args.repeats + 1):
        tag = _sanitize_tag(f"{tag_prefix}.r{r}")
        out_json = results_dir / f"memcached-throughput-report.{tag}.txt.json"

        if not args.force and args.resume and out_json.exists():
            with open(out_json, encoding="utf8") as f:
                data = json.load(f)
            sei_tp = _extract_sei_throughputs(data)
            if (not args.orthrus_sync or "orthrus_sync" in data) and (
                not args.rbv_sync or "rbv_sync" in data
            ):
                if args.dry_run:
                    print(f"[dry-run] would load existing {out_json}", file=sys.stderr)
                else:
                    runs.append(
                        RunResult(
                            repeat=r,
                            tag=tag,
                            throughput_json=str(out_json.relative_to(root)),
                            vanilla=float(data["vanilla"]["throughput"]),
                            sei=sei_tp,
                            orthrus=float(data["orthrus"]["throughput"]),
                            orthrus_sync=(
                                float(data["orthrus_sync"]["throughput"])
                                if "orthrus_sync" in data
                                else None
                            ),
                            rbv=float(data["rbv"]["throughput"]),
                            rbv_sync=(
                                float(data["rbv_sync"]["throughput"])
                                if "rbv_sync" in data
                                else None
                            ),
                        )
                    )
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
            "--sei-variants",
            ",".join(sei_variants),
            "--nclients",
            str(args.nclients),
            "--rps-per-thread",
            "0",
            "--mode",
            "throughput",
            "--tag",
            tag,
            "--orthrus-sync" if args.orthrus_sync else "--no-orthrus-sync",
            "--rbv-sync" if args.rbv_sync else "--no-rbv-sync",
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
        if args.nsets_exp is not None:
            cmd += ["--nsets-exp", str(args.nsets_exp)]
        if args.ngets_exp is not None:
            cmd += ["--ngets-exp", str(args.ngets_exp)]
        if args.read_pct is not None:
            cmd += ["--read-pct", str(args.read_pct)]
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

        if args.dry_run:
            print("[dry-run]", " ".join(cmd), file=sys.stderr)
            continue

        print(f"[{r}/{args.repeats}] running {tag}", file=sys.stderr)
        subprocess.run(cmd, check=True)

        with open(out_json, encoding="utf8") as f:
            data = json.load(f)
        runs.append(
            RunResult(
                repeat=r,
                tag=tag,
                throughput_json=str(out_json.relative_to(root)),
                vanilla=float(data["vanilla"]["throughput"]),
                sei=_extract_sei_throughputs(data),
                orthrus=float(data["orthrus"]["throughput"]),
                orthrus_sync=(
                    float(data["orthrus_sync"]["throughput"])
                    if "orthrus_sync" in data
                    else None
                ),
                rbv=float(data["rbv"]["throughput"]),
                rbv_sync=float(data["rbv_sync"]["throughput"]) if "rbv_sync" in data else None,
            )
        )

    if args.dry_run:
        return 0

    series_values: Dict[str, List[float]] = {"vanilla": [r.vanilla for r in runs]}
    for v in sei_variants:
        sname = sei_series_name[v]
        series_values[sname] = [r.sei[v] for r in runs]
    series_values["orthrus"] = [r.orthrus for r in runs]
    if args.orthrus_sync:
        series_values["orthrus_sync"] = [
            float(r.orthrus_sync) for r in runs if r.orthrus_sync is not None
        ]
    series_values["rbv"] = [r.rbv for r in runs]
    if args.rbv_sync:
        series_values["rbv_sync"] = [
            float(r.rbv_sync) for r in runs if r.rbv_sync is not None
        ]

    medians = {k: _median(vs) for k, vs in series_values.items()}
    per_thread_medians = {k: v / float(args.nclients) for k, v in medians.items()}

    out_json = results_dir / f"memcached-max-throughput.{out_tag}.json"
    out_csv = results_dir / f"memcached-max-throughput.{out_tag}.csv"
    out_txt = results_dir / f"memcached-max-throughput.{out_tag}.txt"

    payload = {
        "meta": {
            "kind": "memcached-max-throughput",
            "git_sha": _git_sha(root),
            "timestamp": timestamp,
            "tag_prefix": tag_prefix,
            "out_tag": out_tag,
        },
        "config": {
            "preset": args.preset,
            "server_ip": args.server_ip,
            "port_start": args.port_start,
            "port_end": args.port_end,
            "nclients": args.nclients,
            "read_pct": args.read_pct,
            "sei_variants": sei_variants,
            "orthrus_sync": args.orthrus_sync,
            "rbv_sync": args.rbv_sync,
            "repeats": args.repeats,
            "pin": args.pin,
            "rps_per_thread": 0,
        },
        "runs": [asdict(r) for r in runs],
        "median": medians,
        "median_per_thread": per_thread_medians,
    }

    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf8")

    with open(out_csv, "w", encoding="utf8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["series", "median_ops_per_sec", "median_ops_per_sec_per_thread"])
        for k in sorted(medians.keys()):
            w.writerow([k, f"{medians[k]:.6f}", f"{per_thread_medians[k]:.6f}"])

    lines: List[str] = []
    lines.append(f"max throughput (rps-per-thread=0, repeats={args.repeats})")
    lines.append(f"nclients={args.nclients}, preset={args.preset}, read_pct={args.read_pct}")
    for k in sorted(medians.keys()):
        lines.append(f"{k}: {medians[k]:.2f} ops/s ({per_thread_medians[k]:.2f} ops/s/thread)")

    cap_per_thread = 100000.0
    maybe_limited = [
        k for k, v in per_thread_medians.items() if v >= cap_per_thread * 0.90
    ]
    if maybe_limited:
        lines.append("")
        lines.append(
            "note: some series are near ~100k ops/s/thread at rps=0; the client uses an internal "
            "upper bound of ~100k ops/s/thread when rps is disabled."
        )
        lines.append(f"near-cap series: {', '.join(sorted(maybe_limited))}")

    out_txt.write_text("\n".join(lines) + "\n", encoding="utf8")

    print(f"Wrote {out_txt}", file=sys.stderr)
    print(f"Wrote {out_csv}", file=sys.stderr)
    print(f"Wrote {out_json}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

