#!/usr/bin/env python3
import argparse
import json
import math
import os
import random
import shlex
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from utils import parse as parse_client_log

SEI_VARIANT_CHOICES = ["er2", "er5", "er10", "dynamicNway", "core", "dynamicCore"]
SEI_VARIANT_ALIASES = {"default": "er2"}


def _normalize_sei_variant(variant: str) -> str:
    v = variant.strip()
    return SEI_VARIANT_ALIASES.get(v, v)


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


def _available_cpus() -> List[int]:
    try:
        cpus = sorted(os.sched_getaffinity(0))  # type: ignore[attr-defined]
        return list(cpus)
    except Exception:
        n = os.cpu_count() or 1
        return list(range(n))


def _format_cpu_list(cpus: Sequence[int]) -> str:
    if not cpus:
        return ""
    xs = sorted(set(cpus))
    parts = []
    start = prev = xs[0]
    for x in xs[1:]:
        if x == prev + 1:
            prev = x
            continue
        parts.append(f"{start}-{prev}" if start != prev else f"{start}")
        start = prev = x
    parts.append(f"{start}-{prev}" if start != prev else f"{start}")
    return ",".join(parts)


def _can_bind_port(port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # Servers bind to INADDR_ANY, so probe with 0.0.0.0 to avoid false positives
            # when a port is already in use on a non-loopback interface.
            s.bind(("0.0.0.0", port))
            return True
    except OSError:
        return False


def _pick_free_port_range(ngroups: int, start: int = 20000, end: int = 40000) -> int:
    if end - start < ngroups + 1:
        raise ValueError("port range too small for ngroups")
    for _ in range(2000):
        base = random.randint(start, end - ngroups - 1)
        if all(_can_bind_port(base + i) for i in range(ngroups)):
            return base
    raise RuntimeError("failed to find a free port range")


def _cmd_with_taskset(cpus: Sequence[int], cmd: Sequence[str], pin: bool) -> List[str]:
    if not pin or not cpus:
        return list(cmd)
    return ["taskset", "-c", _format_cpu_list(cpus), *cmd]


@dataclass(frozen=True)
class CpuLayout:
    server4: List[int]
    server8: List[int]
    rbv_primary: List[int]
    rbv_replica: List[int]
    client: List[int]


@dataclass(frozen=True)
class LogFiles:
    temp_dir: Path
    remote_temp_dir: str
    suffix: str
    remote_client: bool
    client_prefix: str
    run_prefix: str

    def local_client_log(self, name: str) -> Path:
        return self.temp_dir / f"{self.client_prefix}-{name}{self.suffix}.log"

    def client_log_arg(self, name: str) -> str:
        if not self.remote_client:
            return str(self.local_client_log(name))
        return str(
            Path(self.remote_temp_dir) / f"{self.client_prefix}-{name}{self.suffix}.log"
        )

    def run_log(self, name: str) -> Path:
        return self.temp_dir / f"{self.run_prefix}-{name}{self.suffix}.log"


def _choose_cpus(preset: str) -> CpuLayout:
    cpus = _available_cpus()
    n = len(cpus)
    if n <= 0:
        cpus = [0]
        n = 1

    if preset == "fair4c":
        server4_n = min(4, n)
        server4 = cpus[:server4_n]
        server8 = server4

        # RBV: split server4 between primary/replica (keeps total unique cores = 4).
        half = max(1, len(server4) // 2)
        rbv_primary = server4[:half]
        rbv_replica = server4[half:] or rbv_primary

        client = [c for c in cpus if c not in server4] or cpus

        return CpuLayout(
            server4=server4,
            server8=server8,
            rbv_primary=rbv_primary,
            rbv_replica=rbv_replica,
            client=client,
        )

    # Heuristics: try to keep client and servers disjoint when possible.
    server4_n = min(4, max(1, n // 4))
    server8_n = min(8, max(server4_n, n // 2))

    # Vanilla/SEI: use first server4_n cores, client gets the rest.
    server4 = cpus[:server4_n]

    # Orthrus: use first server8_n cores, client gets the rest.
    server8 = cpus[: max(1, min(server8_n, n))]

    # RBV: split up to 2*server4_n cores between primary/replica.
    rbv_primary = cpus[:server4_n]
    rbv_replica = cpus[server4_n : server4_n * 2] or rbv_primary

    # Client: prefer using the remaining cores after reserving server8_n.
    client = cpus[server8_n:] or cpus[server4_n:] or cpus

    return CpuLayout(
        server4=server4,
        server8=server8,
        rbv_primary=rbv_primary,
        rbv_replica=rbv_replica,
        client=client,
    )


def _run_case(
    name: str,
    server_cmds: Sequence[Sequence[str]],
    client_cmd: Sequence[str],
    log_path: Path,
    timeout_sec: int,
    server_start_interval_sec: float = 1.0,
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    procs: List[subprocess.Popen] = []
    with open(log_path, "w", encoding="utf8") as log:
        try:
            for cmd in server_cmds:
                procs.append(
                    subprocess.Popen(
                        list(cmd),
                        stdout=log,
                        stderr=log,
                        text=True,
                    )
                )
                time.sleep(server_start_interval_sec)

            res = subprocess.run(
                list(client_cmd),
                stdout=log,
                stderr=log,
                text=True,
                check=False,
            )
            if res.returncode != 0:
                raise RuntimeError(f"{name}: client exited with {res.returncode}")

            deadline = time.time() + timeout_sec
            for p in procs:
                remaining = max(1, int(deadline - time.time()))
                p.wait(timeout=remaining)
        finally:
            for p in procs:
                if p.poll() is None:
                    p.terminate()
            for p in procs:
                try:
                    p.wait(timeout=3)
                except Exception:
                    pass


def _remove_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(str(path))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run memcached comparison (vanilla/SEI/Orthrus/RBV) in a portable way."
    )

    parser.add_argument(
        "--build-dir",
        default="build",
        help="CMake build directory (default: build).",
    )
    parser.add_argument(
        "--server-ip",
        default="127.0.0.1",
        help=(
            "IPv4 address passed to memcached_client (default: 127.0.0.1). "
            "Note: hostnames are not supported by the client."
        ),
    )
    parser.add_argument(
        "--port-start",
        type=int,
        default=20000,
        help="Start of server port range to use (default: 20000).",
    )
    parser.add_argument(
        "--port-end",
        type=int,
        default=40000,
        help="End of server port range to use (default: 40000).",
    )
    parser.add_argument(
        "--client-ssh",
        default=None,
        help=(
            "If set, run the load-generating client on another host via SSH "
            "(example: user@loadhost). Servers still run locally."
        ),
    )
    parser.add_argument(
        "--client-workdir",
        default=None,
        help="Remote working directory used with --client-ssh (optional).",
    )
    parser.add_argument(
        "--remote-client-bin",
        default=None,
        help=(
            "Path to memcached_client on the remote load host (default: "
            "<build-dir>/ae/memcached/memcached_client)."
        ),
    )
    parser.add_argument(
        "--client-temp-dir",
        default="/tmp/orthrus-memcached",
        help=(
            "Remote directory to place client logs when using --client-ssh "
            "(default: /tmp/orthrus-memcached)."
        ),
    )
    parser.add_argument(
        "--client-pin-cpus",
        default=None,
        help=(
            "If set (remote mode), run the client with 'taskset -c <list>' "
            "(example: 0-7 or 0,2,4,6)."
        ),
    )
    parser.add_argument(
        "--preset",
        choices=["default", "fair4c"],
        default="default",
        help=(
            "CPU/thread preset. 'default' uses the original scaling logic. "
            "'fair4c' uses 4 server cores total, with RBV split across those cores "
            "and Orthrus intended for 3 working cores + 1 validation core."
        ),
    )
    parser.add_argument("--ngroups", type=int, default=3, help="Default group count.")
    parser.add_argument("--vanilla-ngroups", type=int, default=None)
    parser.add_argument("--sei-ngroups", type=int, default=None)
    parser.add_argument("--orthrus-ngroups", type=int, default=None)
    parser.add_argument("--rbv-ngroups", type=int, default=None)
    parser.add_argument(
        "--orthrus-sync",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Also run Orthrus with synchronous validation (SCEE_SYNC_VALIDATE). "
            "Requires build/ae/memcached/memcached_orthrus_sync."
        ),
    )
    parser.add_argument(
        "--rbv-sync",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Also run RBV with synchronous validation "
            "(memcached_rbv_primary --sync)."
        ),
    )
    parser.add_argument("--nclients", type=int, default=16)
    parser.add_argument("--nsets-exp", type=int, default=18)
    parser.add_argument("--ngets-exp", type=int, default=16)
    parser.add_argument("--rps", type=int, default=0)
    parser.add_argument(
        "--read-pct",
        type=float,
        default=None,
        help=(
            "If set, configure memcached_client to be read-heavy: this is the "
            "percentage of GETs among (UPDATE+GET) after the initial SET phase "
            "(example: 95 for ~95%% reads)."
        ),
    )
    parser.add_argument(
        "--rps-per-thread",
        type=float,
        default=None,
        help=(
            "If set, derive per-variant rps arguments so that each client thread "
            "issues approximately this rate (ops/s) for UPDATE/GET. This overrides "
            "--rps unless per-variant --*-rps is explicitly set."
        ),
    )
    parser.add_argument("--vanilla-rps", type=int, default=None)
    parser.add_argument("--sei-rps", type=int, default=None)
    parser.add_argument("--orthrus-rps", type=int, default=None)
    parser.add_argument("--rbv-rps", type=int, default=None)
    parser.add_argument(
        "--sei-variants",
        default=None,
        help=(
            "Comma-separated SEI variants to run. If set, runs all listed variants "
            "and overrides --sei-variant. Example: er2,er5,dynamicNway"
        ),
    )
    parser.add_argument(
        "--sei-variant",
        type=_normalize_sei_variant,
        choices=SEI_VARIANT_CHOICES,
        default="er2",
        help="Which SEI memcached server variant to run.",
    )
    parser.add_argument(
        "--tag",
        default=None,
        help=(
            "If set, write outputs to results/memcached-*-report.<tag>.txt "
            "(throughput also writes a .json sidecar) and write a "
            "results/memcached-config.<tag>.txt config log."
        ),
    )
    parser.add_argument("--pin", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--mode",
        choices=["throughput", "memory", "all"],
        default="throughput",
    )
    parser.add_argument(
        "--timeout-sec",
        type=int,
        default=1800,
        help="Timeout for each case (default: 1800s).",
    )
    args = parser.parse_args()

    if args.port_start <= 0 or args.port_end <= 0:
        raise ValueError("--port-start/--port-end must be > 0")
    if args.port_end <= args.port_start:
        raise ValueError("--port-end must be > --port-start")
    try:
        socket.inet_aton(args.server_ip)
    except OSError as e:
        raise ValueError(
            f"--server-ip must be an IPv4 address string (got {args.server_ip!r})"
        ) from e

    if args.client_ssh is not None and args.remote_client_bin is None:
        args.remote_client_bin = str(
            Path(args.build_dir) / "ae" / "memcached" / "memcached_client"
        )

    if args.preset == "fair4c":
        if args.vanilla_ngroups is None:
            args.vanilla_ngroups = 8
        if args.sei_ngroups is None:
            args.sei_ngroups = 8
        if args.orthrus_ngroups is None:
            args.orthrus_ngroups = 6
        if args.rbv_ngroups is None:
            args.rbv_ngroups = 4
    else:
        if args.vanilla_ngroups is None:
            args.vanilla_ngroups = args.ngroups
        if args.sei_ngroups is None:
            args.sei_ngroups = args.ngroups
        if args.orthrus_ngroups is None:
            args.orthrus_ngroups = args.ngroups
        if args.rbv_ngroups is None:
            args.rbv_ngroups = args.ngroups

    if args.rps_per_thread is not None and args.rps_per_thread < 0:
        raise ValueError("--rps-per-thread must be >= 0")

    client_read_pct_arg: List[str] = []
    if args.read_pct is not None:
        read_pct = float(args.read_pct)
        if read_pct <= 1.0:
            read_pct *= 100.0
        if not (read_pct > 0.0 and read_pct <= 100.0):
            raise ValueError(
                "--read-pct must be in (0,100] (or provide a ratio in (0,1])."
            )
        args.read_pct = read_pct
        client_read_pct_arg = [str(read_pct)]

    def _derive_rps(*, override: Optional[int], ngroups: int) -> int:
        if override is not None:
            if override < 0:
                raise ValueError("rps override must be >= 0")
            return override
        if args.rps_per_thread is None:
            return args.rps
        if args.rps_per_thread <= 0:
            return 0
        # Client interprets rps as "per-group" and uses integer arithmetic:
        #   rps_per_thread = floor(rps * ngroups / nclients)
        # Choose rps so floor(...) >= requested rps_per_thread.
        return max(1, int(math.ceil(args.rps_per_thread * args.nclients / ngroups)))

    vanilla_rps = _derive_rps(override=args.vanilla_rps, ngroups=args.vanilla_ngroups)
    sei_rps = _derive_rps(override=args.sei_rps, ngroups=args.sei_ngroups)
    orthrus_rps = _derive_rps(override=args.orthrus_rps, ngroups=args.orthrus_ngroups)
    rbv_rps = _derive_rps(override=args.rbv_rps, ngroups=args.rbv_ngroups)

    allowed_sei_variants = set(SEI_VARIANT_CHOICES)

    def _parse_sei_variants_arg(s: str) -> List[str]:
        parts = [p.strip() for p in s.split(",") if p.strip()]
        if not parts:
            raise ValueError("--sei-variants must be a non-empty comma-separated list")
        out: List[str] = []
        seen = set()
        for p in parts:
            p = _normalize_sei_variant(p)
            if p not in allowed_sei_variants:
                raise ValueError(f"Unknown SEI variant in --sei-variants: {p}")
            if p in seen:
                continue
            seen.add(p)
            out.append(p)
        return out

    sei_variants = (
        _parse_sei_variants_arg(args.sei_variants)
        if args.sei_variants is not None
        else [args.sei_variant]
    )
    if len(sei_variants) > 1 and args.mode in ("memory", "all"):
        raise ValueError("--sei-variants is only supported with --mode throughput")

    root = Path(__file__).resolve().parents[2]
    build_dir = (root / args.build_dir).resolve()
    temp_dir = root / "temp"
    results_dir = root / "results"
    temp_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    mem_dir = build_dir / "ae" / "memcached"
    client_bin = mem_dir / "memcached_client"

    bins = {
        "vanilla": mem_dir / "memcached_vanilla",
        "sei": mem_dir / "memcached_sei",
        "sei_er2": mem_dir / "memcached_sei_er2",
        "sei_er5": mem_dir / "memcached_sei_er5",
        "sei_er10": mem_dir / "memcached_sei_er10",
        "sei_dynamicNway": mem_dir / "memcached_sei_dynamic_nway",
        "sei_core": mem_dir / "memcached_sei_core",
        "sei_dynamicCore": mem_dir / "memcached_sei_dynamic_core",
        "orthrus": mem_dir / "memcached_orthrus",
        "orthrus_sync": mem_dir / "memcached_orthrus_sync",
        "rbv_primary": mem_dir / "memcached_rbv_primary",
        "rbv_replica": mem_dir / "memcached_rbv_replica",
        "vanilla_mem": mem_dir / "memcached_vanilla_mem",
        "sei_mem": mem_dir / "memcached_sei_mem",
        "sei_er2_mem": mem_dir / "memcached_sei_er2_mem",
        "sei_er5_mem": mem_dir / "memcached_sei_er5_mem",
        "sei_er10_mem": mem_dir / "memcached_sei_er10_mem",
        "sei_dynamicNway_mem": mem_dir / "memcached_sei_dynamic_nway_mem",
        "sei_core_mem": mem_dir / "memcached_sei_core_mem",
        "sei_dynamicCore_mem": mem_dir / "memcached_sei_dynamic_core_mem",
        "orthrus_mem": mem_dir / "memcached_orthrus_mem",
        "orthrus_sync_mem": mem_dir / "memcached_orthrus_sync_mem",
        "rbv_primary_mem": mem_dir / "memcached_rbv_primary_mem",
        "rbv_replica_mem": mem_dir / "memcached_rbv_replica_mem",
    }

    def _sei_bin_key(variant: str) -> str:
        return {
            "er2": "sei_er2",
            "er5": "sei_er5",
            "er10": "sei_er10",
            "dynamicNway": "sei_dynamicNway",
            "core": "sei_core",
            "dynamicCore": "sei_dynamicCore",
        }[variant]

    def _sei_mem_bin_key(variant: str) -> str:
        return {
            "er2": "sei_er2_mem",
            "er5": "sei_er5_mem",
            "er10": "sei_er10_mem",
            "dynamicNway": "sei_dynamicNway_mem",
            "core": "sei_core_mem",
            "dynamicCore": "sei_dynamicCore_mem",
        }[variant]

    sei_bin_keys = [_sei_bin_key(v) for v in sei_variants]
    sei_mem_bin_key = _sei_mem_bin_key(sei_variants[0])

    _require_file(client_bin)
    required = set()
    if args.mode in ("throughput", "all"):
        required |= (
            {"vanilla", "orthrus", "rbv_primary", "rbv_replica"} | set(sei_bin_keys)
        )
        if args.orthrus_sync:
            required.add("orthrus_sync")
    if args.mode in ("memory", "all"):
        required |= {
            "vanilla_mem",
            sei_mem_bin_key,
            "orthrus_mem",
            "rbv_primary_mem",
            "rbv_replica_mem",
        }
        if args.orthrus_sync:
            required.add("orthrus_sync_mem")
    for k in sorted(required):
        _require_file(bins[k])

    remote_client = args.client_ssh is not None

    def _pick_ports(ngroups: int) -> int:
        return _pick_free_port_range(ngroups, start=args.port_start, end=args.port_end)

    def _remote_shell(cmd: str) -> List[str]:
        if args.client_ssh is None:
            raise ValueError("remote shell requested without --client-ssh")
        return ["ssh", args.client_ssh, cmd]

    def _remote_cmd(tokens: Sequence[str]) -> List[str]:
        cmd = shlex.join(list(tokens))
        if args.client_workdir:
            cmd = f"cd {shlex.quote(args.client_workdir)} && {cmd}"
        return _remote_shell(cmd)

    def _remote_mkdir(path: str) -> None:
        subprocess.run(_remote_shell(f"mkdir -p {shlex.quote(path)}"), check=True)

    def _remote_rm(path: str) -> None:
        subprocess.run(_remote_shell(f"rm -f {shlex.quote(path)}"), check=True)

    def _remote_check_executable(path: str) -> None:
        cmd = f"test -x {shlex.quote(path)}"
        if args.client_workdir:
            cmd = f"cd {shlex.quote(args.client_workdir)} && {cmd}"
        subprocess.run(_remote_shell(cmd), check=True)

    def _remote_fetch(remote_path: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            subprocess.run(
                _remote_shell(f"cat {shlex.quote(remote_path)}"),
                check=True,
                stdout=f,
            )

    def _client_cmd(cmd: Sequence[str]) -> List[str]:
        if not remote_client:
            return _cmd_with_taskset(cpus.client, cmd, args.pin)
        remote_tokens: List[str] = list(cmd)
        if args.client_pin_cpus:
            remote_tokens = ["taskset", "-c", args.client_pin_cpus, *remote_tokens]
        return _remote_cmd(remote_tokens)

    if remote_client:
        _remote_mkdir(args.client_temp_dir)
        if args.remote_client_bin is None:
            raise ValueError("--remote-client-bin must be set when using --client-ssh")
        _remote_check_executable(args.remote_client_bin)

    cpus = _choose_cpus(args.preset)
    orthrus_work_cpus = cpus.server8[:-1] if len(cpus.server8) > 1 else cpus.server8
    orthrus_val_cpus = cpus.server8[-1:] if cpus.server8 else []
    client_exec = str(args.remote_client_bin) if remote_client else str(client_bin)

    def _pick_disjoint_ports(ngroups: int) -> Tuple[int, int]:
        replica_port = _pick_ports(ngroups)
        while True:
            port = _pick_ports(ngroups)
            if port + ngroups - 1 < replica_port or replica_port + ngroups - 1 < port:
                return port, replica_port

    def _prepare_client_log(logs: LogFiles, name: str) -> None:
        _remove_if_exists(logs.local_client_log(name))
        if logs.remote_client:
            _remote_rm(logs.client_log_arg(name))

    def _fetch_client_log(logs: LogFiles, name: str) -> None:
        if logs.remote_client:
            _remote_fetch(logs.client_log_arg(name), logs.local_client_log(name))

    def _client_args(port: int, client_log: str, ngroups: int, rps: int) -> List[str]:
        return [
            client_exec,
            args.server_ip,
            str(port),
            client_log,
            str(ngroups),
            str(args.nclients),
            str(args.nsets_exp),
            str(args.ngets_exp),
            str(rps),
            *client_read_pct_arg,
        ]

    def _orthrus_server_cmd(bin_path: Path, port: int) -> List[str]:
        cmd: List[str] = [str(bin_path), str(port), str(args.orthrus_ngroups)]
        if args.preset == "fair4c":
            cmd = [
                "env",
                f"SCEE_WORK_CPUSET={_format_cpu_list(orthrus_work_cpus)}",
                f"SCEE_VALIDATION_CPUSET={_format_cpu_list(orthrus_val_cpus)}",
                *cmd,
            ]
        return cmd

    def _rbv_primary_cmd(bin_path: Path, port: int, replica_port: int, mode: str) -> List[str]:
        if mode not in ("--async", "--sync"):
            raise ValueError(f"invalid rbv primary mode: {mode!r}")
        return [
            str(bin_path),
            str(port),
            str(args.rbv_ngroups),
            str(replica_port),
            "127.0.0.1",
            mode,
        ]

    if args.tag is not None:
        if "/" in args.tag or "\\" in args.tag:
            raise ValueError("--tag must not contain path separators")
    print(
        f"CPU layout: server4={_format_cpu_list(cpus.server4)} "
        f"server8={_format_cpu_list(cpus.server8)} "
        f"rbv_primary={_format_cpu_list(cpus.rbv_primary)} "
        f"rbv_replica={_format_cpu_list(cpus.rbv_replica)} "
        f"client={_format_cpu_list(cpus.client)} "
        f"(preset={args.preset}, vanilla_ngroups={args.vanilla_ngroups}, "
        f"sei_ngroups={args.sei_ngroups}, orthrus_ngroups={args.orthrus_ngroups}, "
        f"rbv_ngroups={args.rbv_ngroups})",
        file=sys.stderr,
    )
    if args.preset == "fair4c":
        print(
            f"Orthrus: work={_format_cpu_list(orthrus_work_cpus)} "
            f"validation={_format_cpu_list(orthrus_val_cpus)}",
            file=sys.stderr,
        )

    if args.tag is not None:
        config_path = results_dir / f"memcached-config.{args.tag}.txt"
        libsei_root = (root / ".." / "libsei-gcc").resolve()
        libsei_build_dir_by_variant = {
            "er2": libsei_root / "build_er2_nomig",
            "er5": libsei_root / "build_er5_nomig",
            "er10": libsei_root / "build_er10_nomig",
            "dynamicNway": libsei_root / "build_dyn_nway_er5_rb",
            "core": libsei_root / "build_core1_only",
            "dynamicCore": libsei_root / "build_dyn_core_rb",
        }
        libsei_make_flags_by_variant = {
            "er2": "EXECUTION_REDUNDANCY=2 (no ROLLBACK, no EXECUTION_CORE_REDUNDANCY)",
            "er5": "EXECUTION_REDUNDANCY=5 (no ROLLBACK, no EXECUTION_CORE_REDUNDANCY)",
            "er10": "EXECUTION_REDUNDANCY=10 (no ROLLBACK, no EXECUTION_CORE_REDUNDANCY)",
            "dynamicNway": "ROLLBACK=1 EXECUTION_REDUNDANCY=5 (dynamic redundancy via __begin_n)",
            "core": "ROLLBACK=1 EXECUTION_CORE_REDUNDANCY=1 (core redundancy)",
            "dynamicCore": "ROLLBACK=1 EXECUTION_REDUNDANCY=2 (dynamic core migration via __begin_core_redundancy)",
        }

        def _rel(p: Path) -> str:
            try:
                return str(p.relative_to(root))
            except Exception:
                return str(p)

        orthrus_sha = _git_sha(root)
        libsei_sha = _git_sha(libsei_root)

        config_lines = [
            f"preset={args.preset}",
            f"server_ip={args.server_ip}",
            f"port_range={args.port_start}-{args.port_end}",
            f"client_ssh={args.client_ssh or '(local)'}",
            f"client_workdir={args.client_workdir or '(none)'}",
            f"remote_client_bin={args.remote_client_bin or '(local)'}",
            f"client_temp_dir={args.client_temp_dir if remote_client else '(local)'}",
            f"client_pin_cpus={args.client_pin_cpus or '(none)'}",
            (
                "cpu_layout: "
                f"server4={_format_cpu_list(cpus.server4)} "
                f"server8={_format_cpu_list(cpus.server8)} "
                f"rbv_primary={_format_cpu_list(cpus.rbv_primary)} "
                f"rbv_replica={_format_cpu_list(cpus.rbv_replica)} "
                f"client={_format_cpu_list(cpus.client)}"
            ),
            (
                "ngroups: "
                f"vanilla={args.vanilla_ngroups} "
                f"sei={args.sei_ngroups} "
                f"orthrus={args.orthrus_ngroups} "
                f"rbv={args.rbv_ngroups}"
            ),
            f"rps_default={args.rps}",
            f"rps_per_thread={args.rps_per_thread}",
            (
                "rps_by_variant: "
                f"vanilla={vanilla_rps} sei={sei_rps} orthrus={orthrus_rps} rbv={rbv_rps}"
            ),
            f"read_pct={args.read_pct if args.read_pct is not None else '(disabled)'}",
            f"orthrus_sync={args.orthrus_sync}",
            f"rbv_sync={args.rbv_sync}",
            (
                "orthrus_env: "
                f"SCEE_WORK_CPUSET={_format_cpu_list(orthrus_work_cpus)} "
                f"SCEE_VALIDATION_CPUSET={_format_cpu_list(orthrus_val_cpus)}"
            )
            if args.preset == "fair4c"
            else "orthrus_env: (none)",
            f"sei_variants={','.join(sei_variants)}",
            "sei_server_binaries: "
            + " ".join(f"{v}={_rel(bins[_sei_bin_key(v)])}" for v in sei_variants),
            f"orthrus_server_binary={_rel(bins['orthrus'])}",
            f"orthrus_sync_server_binary={_rel(bins['orthrus_sync'])}"
            if args.orthrus_sync
            else "orthrus_sync_server_binary=(disabled)",
            "libsei_build_dirs: "
            + " ".join(f"{v}={libsei_build_dir_by_variant[v]}" for v in sei_variants),
            "libsei_make_flags: "
            + " | ".join(f"{v}={libsei_make_flags_by_variant[v]}" for v in sei_variants),
            f"sha: Orthrus={orthrus_sha}",
            f"sha: libsei-gcc={libsei_sha}",
        ]
        config_path.write_text("\n".join(config_lines) + "\n", encoding="utf8")

    def run_throughput() -> None:
        out_txt = results_dir / "memcached-throughput-report.txt"
        if args.tag is not None:
            out_txt = results_dir / f"memcached-throughput-report.{args.tag}.txt"
        out_txt.write_text("", encoding="utf8")
        suffix = f".{args.tag}" if args.tag is not None else ""
        logs = LogFiles(
            temp_dir=temp_dir,
            remote_temp_dir=args.client_temp_dir,
            suffix=suffix,
            remote_client=remote_client,
            client_prefix="memcached-throughput-client",
            run_prefix="run-memcached-throughput",
        )

        # vanilla
        port = _pick_ports(args.vanilla_ngroups)
        _prepare_client_log(logs, "vanilla")
        _run_case(
            "vanilla",
            [
                _cmd_with_taskset(
                    cpus.server4,
                    [str(bins["vanilla"]), str(port), str(args.vanilla_ngroups)],
                    args.pin,
                )
            ],
            _client_cmd(
                _client_args(
                    port,
                    logs.client_log_arg("vanilla"),
                    args.vanilla_ngroups,
                    vanilla_rps,
                )
            ),
            logs.run_log("vanilla"),
            args.timeout_sec,
        )
        _fetch_client_log(logs, "vanilla")

        def _sei_case_name(variant: str) -> str:
            if len(sei_variants) == 1:
                return "sei"
            return f"sei_{variant}"

        sei_client_logs: Dict[str, Path] = {}
        for variant in sei_variants:
            sei_name = _sei_case_name(variant)
            port = _pick_ports(args.sei_ngroups)
            _prepare_client_log(logs, sei_name)
            _run_case(
                sei_name,
                [
                    _cmd_with_taskset(
                        cpus.server4,
                        [
                            str(bins[_sei_bin_key(variant)]),
                            str(port),
                            str(args.sei_ngroups),
                        ],
                        args.pin,
                    )
                ],
                _client_cmd(
                    _client_args(
                        port,
                        logs.client_log_arg(sei_name),
                        args.sei_ngroups,
                        sei_rps,
                    )
                ),
                logs.run_log(sei_name),
                args.timeout_sec,
            )
            _fetch_client_log(logs, sei_name)
            sei_client_logs[variant] = logs.local_client_log(sei_name)

        # orthrus
        port = _pick_ports(args.orthrus_ngroups)
        _prepare_client_log(logs, "orthrus")
        _run_case(
            "orthrus",
            [
                _cmd_with_taskset(
                    cpus.server8,
                    _orthrus_server_cmd(bins["orthrus"], port),
                    args.pin,
                )
            ],
            _client_cmd(
                _client_args(
                    port,
                    logs.client_log_arg("orthrus"),
                    args.orthrus_ngroups,
                    orthrus_rps,
                )
            ),
            logs.run_log("orthrus"),
            args.timeout_sec,
        )
        _fetch_client_log(logs, "orthrus")

        if args.orthrus_sync:
            # Orthrus (sync validation)
            port = _pick_ports(args.orthrus_ngroups)
            _prepare_client_log(logs, "orthrus_sync")
            _run_case(
                "orthrus_sync",
                [
                    _cmd_with_taskset(
                        cpus.server8,
                        _orthrus_server_cmd(bins["orthrus_sync"], port),
                        args.pin,
                    )
                ],
                _client_cmd(
                    _client_args(
                        port,
                        logs.client_log_arg("orthrus_sync"),
                        args.orthrus_ngroups,
                        orthrus_rps,
                    )
                ),
                logs.run_log("orthrus_sync"),
                args.timeout_sec,
            )
            _fetch_client_log(logs, "orthrus_sync")

        # rbv (async baseline)
        port, replica_port = _pick_disjoint_ports(args.rbv_ngroups)
        _prepare_client_log(logs, "rbv")
        _run_case(
            "rbv",
            [
                _cmd_with_taskset(
                    cpus.rbv_replica,
                    [str(bins["rbv_replica"]), str(replica_port), str(args.rbv_ngroups)],
                    args.pin,
                ),
                _cmd_with_taskset(
                    cpus.rbv_primary,
                    _rbv_primary_cmd(bins["rbv_primary"], port, replica_port, "--async"),
                    args.pin,
                ),
            ],
            _client_cmd(
                _client_args(
                    port,
                    logs.client_log_arg("rbv"),
                    args.rbv_ngroups,
                    rbv_rps,
                )
            ),
            logs.run_log("rbv"),
            args.timeout_sec,
            server_start_interval_sec=2.0,
        )
        _fetch_client_log(logs, "rbv")

        # rbv (sync validation)
        if args.rbv_sync:
            port, replica_port = _pick_disjoint_ports(args.rbv_ngroups)
            _prepare_client_log(logs, "rbv_sync")
            _run_case(
                "rbv_sync",
                [
                    _cmd_with_taskset(
                        cpus.rbv_replica,
                        [str(bins["rbv_replica"]), str(replica_port), str(args.rbv_ngroups)],
                        args.pin,
                    ),
                    _cmd_with_taskset(
                        cpus.rbv_primary,
                        _rbv_primary_cmd(bins["rbv_primary"], port, replica_port, "--sync"),
                        args.pin,
                    ),
                ],
                _client_cmd(
                    _client_args(
                        port,
                        logs.client_log_arg("rbv_sync"),
                        args.rbv_ngroups,
                        rbv_rps,
                    )
                ),
                logs.run_log("rbv_sync"),
                args.timeout_sec,
                server_start_interval_sec=2.0,
            )
            _fetch_client_log(logs, "rbv_sync")

        def _parse_one(path: Path) -> Dict[str, object]:
            parsed = parse_client_log(str(path))
            if len(parsed) != 1:
                raise RuntimeError(f"unexpected client log format: {path} ({len(parsed)} blocks)")
            return parsed[0]

        out: Dict[str, object] = {}
        out["vanilla"] = _parse_one(logs.local_client_log("vanilla"))

        sei_out: Dict[str, object] = {v: _parse_one(p) for v, p in sei_client_logs.items()}
        if len(sei_variants) == 1:
            out["sei"] = sei_out[sei_variants[0]]
        else:
            out["sei"] = sei_out

        out["orthrus"] = _parse_one(logs.local_client_log("orthrus"))
        if args.orthrus_sync:
            out["orthrus_sync"] = _parse_one(logs.local_client_log("orthrus_sync"))
        out["rbv"] = _parse_one(logs.local_client_log("rbv"))
        if args.rbv_sync:
            out["rbv_sync"] = _parse_one(logs.local_client_log("rbv_sync"))

        lines: List[str] = []
        lines.append("vanilla running")
        lines.append(f"throughput: {out['vanilla']['throughput']}")  # type: ignore[index]
        if len(sei_variants) == 1:
            lines.append("sei running")
            lines.append(f"throughput: {out['sei']['throughput']}")  # type: ignore[index]
        else:
            for v in sei_variants:
                lines.append(f"sei_{v} running")
                lines.append(f"throughput: {sei_out[v]['throughput']}")  # type: ignore[index]
        lines.append("orthrus running")
        lines.append(f"throughput: {out['orthrus']['throughput']}")  # type: ignore[index]
        if args.orthrus_sync:
            lines.append("orthrus_sync running")
            lines.append(f"throughput: {out['orthrus_sync']['throughput']}")  # type: ignore[index]
        lines.append("rbv running")
        lines.append(f"throughput: {out['rbv']['throughput']}")  # type: ignore[index]
        if args.rbv_sync:
            lines.append("rbv_sync running")
            lines.append(f"throughput: {out['rbv_sync']['throughput']}")  # type: ignore[index]
        out_txt.write_text("\n".join(lines) + "\n", encoding="utf8")

        out_json = Path(f"{out_txt}.json")
        out_json.write_text(
            json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf8"
        )
        print(f"Wrote {out_txt}", file=sys.stderr)

    def run_memory() -> None:
        def move_if_exists(src: Path, dst: Path) -> None:
            if src.exists():
                dst.parent.mkdir(parents=True, exist_ok=True)
                if dst.exists():
                    dst.unlink()
                src.rename(dst)

        suffix = f".{args.tag}" if args.tag is not None else ""
        logs = LogFiles(
            temp_dir=temp_dir,
            remote_temp_dir=args.client_temp_dir,
            suffix=suffix,
            remote_client=remote_client,
            client_prefix="memcached-mem-client",
            run_prefix="run-memcached-mem",
        )

        def mem_status_log(name: str) -> Path:
            return temp_dir / f"memcached-memory_status-{name}{suffix}.log"

        # Ensure old logs don't get appended to (client opens output in append mode).
        names = ["vanilla", "sei", "orthrus", "rbv"]
        if args.orthrus_sync:
            names.append("orthrus_sync")
        for name in names:
            _prepare_client_log(logs, name)

        # vanilla
        port = _pick_ports(args.vanilla_ngroups)
        _remove_if_exists(root / "memcached-memory_status-vanilla.log")
        _run_case(
            "vanilla_mem",
            [
                _cmd_with_taskset(
                    cpus.server4,
                    [str(bins["vanilla_mem"]), str(port), str(args.vanilla_ngroups)],
                    args.pin,
                )
            ],
            _client_cmd(
                _client_args(
                    port,
                    logs.client_log_arg("vanilla"),
                    args.vanilla_ngroups,
                    vanilla_rps,
                )
            ),
            logs.run_log("vanilla"),
            args.timeout_sec,
        )
        _fetch_client_log(logs, "vanilla")
        move_if_exists(
            root / "memcached-memory_status-vanilla.log",
            mem_status_log("vanilla"),
        )

        # sei
        port = _pick_ports(args.sei_ngroups)
        _remove_if_exists(root / "memcached-memory_status-sei.log")
        _run_case(
            "sei_mem",
            [
                _cmd_with_taskset(
                    cpus.server4,
                    [str(bins[sei_mem_bin_key]), str(port), str(args.sei_ngroups)],
                    args.pin,
                )
            ],
            _client_cmd(
                _client_args(
                    port,
                    logs.client_log_arg("sei"),
                    args.sei_ngroups,
                    sei_rps,
                )
            ),
            logs.run_log("sei"),
            args.timeout_sec,
        )
        _fetch_client_log(logs, "sei")
        move_if_exists(
            root / "memcached-memory_status-sei.log",
            mem_status_log("sei"),
        )

        # orthrus
        port = _pick_ports(args.orthrus_ngroups)
        _remove_if_exists(root / "memcached-memory_status-orthrus.log")
        _run_case(
            "orthrus_mem",
            [
                _cmd_with_taskset(
                    cpus.server8,
                    _orthrus_server_cmd(bins["orthrus_mem"], port),
                    args.pin,
                )
            ],
            _client_cmd(
                _client_args(
                    port,
                    logs.client_log_arg("orthrus"),
                    args.orthrus_ngroups,
                    orthrus_rps,
                )
            ),
            logs.run_log("orthrus"),
            args.timeout_sec,
        )
        _fetch_client_log(logs, "orthrus")
        move_if_exists(
            root / "memcached-memory_status-orthrus.log",
            mem_status_log("orthrus"),
        )

        if args.orthrus_sync:
            # orthrus (sync validation)
            port = _pick_ports(args.orthrus_ngroups)
            _remove_if_exists(root / "memcached-memory_status-orthrus.log")
            _run_case(
                "orthrus_sync_mem",
                [
                    _cmd_with_taskset(
                        cpus.server8,
                        _orthrus_server_cmd(bins["orthrus_sync_mem"], port),
                        args.pin,
                    )
                ],
                _client_cmd(
                    _client_args(
                        port,
                        logs.client_log_arg("orthrus_sync"),
                        args.orthrus_ngroups,
                        orthrus_rps,
                    )
                ),
                logs.run_log("orthrus_sync"),
                args.timeout_sec,
            )
            _fetch_client_log(logs, "orthrus_sync")
            move_if_exists(
                root / "memcached-memory_status-orthrus.log",
                mem_status_log("orthrus_sync"),
        )

        # rbv
        port, replica_port = _pick_disjoint_ports(args.rbv_ngroups)
        _remove_if_exists(root / "memcached-memory_status-rbv-primary.log")
        _remove_if_exists(root / "memcached-memory_status-rbv-replica.log")
        _run_case(
            "rbv_mem",
            [
                _cmd_with_taskset(
                    cpus.rbv_replica,
                    [str(bins["rbv_replica_mem"]), str(replica_port), str(args.rbv_ngroups)],
                    args.pin,
                ),
                _cmd_with_taskset(
                    cpus.rbv_primary,
                    _rbv_primary_cmd(
                        bins["rbv_primary_mem"],
                        port,
                        replica_port,
                        "--sync" if args.rbv_sync else "--async",
                    ),
                    args.pin,
                ),
            ],
            _client_cmd(
                _client_args(
                    port,
                    logs.client_log_arg("rbv"),
                    args.rbv_ngroups,
                    rbv_rps,
                )
            ),
            logs.run_log("rbv"),
            args.timeout_sec,
            server_start_interval_sec=2.0,
        )
        _fetch_client_log(logs, "rbv")
        move_if_exists(
            root / "memcached-memory_status-rbv-primary.log",
            mem_status_log("rbv-primary"),
        )
        move_if_exists(
            root / "memcached-memory_status-rbv-replica.log",
            mem_status_log("rbv-replica"),
        )

        out = results_dir / "memcached-mem-report.txt"
        if args.tag is not None:
            out = results_dir / f"memcached-mem-report.{args.tag}.txt"
        with open(out, "w", encoding="utf8") as f:
            subprocess.run(
                [
                    sys.executable,
                    str(root / "scripts" / "memory.py"),
                    "--input-raw",
                    str(mem_status_log("vanilla")),
                    "--input-sei",
                    str(mem_status_log("sei")),
                    "--input-scee",
                    str(mem_status_log("orthrus")),
                    *(
                        ["--input-scee-sync", str(mem_status_log("orthrus_sync"))]
                        if args.orthrus_sync
                        else []
                    ),
                    "--input-rbv",
                    str(mem_status_log("rbv-primary")),
                    "--input-rbv",
                    str(mem_status_log("rbv-replica")),
                ],
                check=True,
                cwd=str(root),
                stdout=f,
                stderr=subprocess.STDOUT,
            )
        print(f"Wrote {out}", file=sys.stderr)

    if args.mode in ("throughput", "all"):
        run_throughput()
    if args.mode in ("memory", "all"):
        run_memory()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
