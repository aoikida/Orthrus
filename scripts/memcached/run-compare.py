#!/usr/bin/env python3
import argparse
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
from typing import List, Optional, Sequence


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
    parser.add_argument("--nclients", type=int, default=16)
    parser.add_argument("--nsets-exp", type=int, default=18)
    parser.add_argument("--ngets-exp", type=int, default=16)
    parser.add_argument("--rps", type=int, default=0)
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
        "--sei-variant",
        choices=["default", "er2", "er5", "er10", "dynamicNway", "dynamicCore"],
        default="default",
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
        "sei_dynamicCore_mem": mem_dir / "memcached_sei_dynamic_core_mem",
        "orthrus_mem": mem_dir / "memcached_orthrus_mem",
        "orthrus_sync_mem": mem_dir / "memcached_orthrus_sync_mem",
        "rbv_primary_mem": mem_dir / "memcached_rbv_primary_mem",
        "rbv_replica_mem": mem_dir / "memcached_rbv_replica_mem",
    }

    sei_bin_key = {
        "default": "sei",
        "er2": "sei_er2",
        "er5": "sei_er5",
        "er10": "sei_er10",
        "dynamicNway": "sei_dynamicNway",
        "dynamicCore": "sei_dynamicCore",
    }[args.sei_variant]
    sei_mem_bin_key = {
        "default": "sei_mem",
        "er2": "sei_er2_mem",
        "er5": "sei_er5_mem",
        "er10": "sei_er10_mem",
        "dynamicNway": "sei_dynamicNway_mem",
        "dynamicCore": "sei_dynamicCore_mem",
    }[args.sei_variant]

    _require_file(client_bin)
    required = set()
    if args.mode in ("throughput", "all"):
        required |= {"vanilla", sei_bin_key, "orthrus", "rbv_primary", "rbv_replica"}
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
        libsei_build_dir = {
            "default": libsei_root / "build",
            "er2": libsei_root / "build_er2_nomig",
            "er5": libsei_root / "build_er5_nomig",
            "er10": libsei_root / "build_er10_nomig",
            "dynamicNway": libsei_root / "build_dyn_nway_er5_rb",
            "dynamicCore": libsei_root / "build_dyn_core_rb",
        }[args.sei_variant]
        libsei_make_flags = {
            "default": "(see libsei-gcc/build configuration)",
            "er2": "EXECUTION_REDUNDANCY=2 (no ROLLBACK, no EXECUTION_CORE_REDUNDANCY)",
            "er5": "EXECUTION_REDUNDANCY=5 (no ROLLBACK, no EXECUTION_CORE_REDUNDANCY)",
            "er10": "EXECUTION_REDUNDANCY=10 (no ROLLBACK, no EXECUTION_CORE_REDUNDANCY)",
            "dynamicNway": "ROLLBACK=1 EXECUTION_REDUNDANCY=5 (dynamic redundancy via __begin_n)",
            "dynamicCore": "ROLLBACK=1 (dynamic core migration via __begin_core_redundancy)",
        }[args.sei_variant]

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

        def _rel(p: Path) -> str:
            try:
                return str(p.relative_to(root))
            except Exception:
                return str(p)

        orthus_sha = _git_sha(root)
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
            f"rps_by_variant: vanilla={vanilla_rps} sei={sei_rps} orthrus={orthrus_rps} rbv={rbv_rps}",
            f"orthrus_sync={args.orthrus_sync}",
            (
                "orthus_env: "
                f"SCEE_WORK_CPUSET={_format_cpu_list(orthrus_work_cpus)} "
                f"SCEE_VALIDATION_CPUSET={_format_cpu_list(orthrus_val_cpus)}"
            )
            if args.preset == "fair4c"
            else "orthus_env: (none)",
            f"sei_variant={args.sei_variant}",
            f"sei_server_binary={_rel(bins[sei_bin_key])}",
            f"orthrus_server_binary={_rel(bins['orthrus'])}",
            f"orthrus_sync_server_binary={_rel(bins['orthrus_sync'])}"
            if args.orthrus_sync
            else "orthrus_sync_server_binary=(disabled)",
            f"libsei_build_dir={libsei_build_dir}",
            f"libsei_make_flags={libsei_make_flags}",
            f"sha: Orthrus={orthus_sha}",
            f"sha: libsei-gcc={libsei_sha}",
        ]
        config_path.write_text("\n".join(config_lines) + "\n", encoding="utf8")

    def run_throughput() -> None:
        out_txt = results_dir / "memcached-throughput-report.txt"
        if args.tag is not None:
            out_txt = results_dir / f"memcached-throughput-report.{args.tag}.txt"
        out_txt.write_text("", encoding="utf8")
        suffix = f".{args.tag}" if args.tag is not None else ""

        def client_log_local(name: str) -> Path:
            return temp_dir / f"memcached-throughput-client-{name}{suffix}.log"

        def client_log_arg(name: str) -> str:
            if not remote_client:
                return str(client_log_local(name))
            return str(Path(args.client_temp_dir) / f"memcached-throughput-client-{name}{suffix}.log")

        def run_log(name: str) -> Path:
            return temp_dir / f"run-memcached-throughput-{name}{suffix}.log"

        # vanilla
        port = _pick_ports(args.vanilla_ngroups)
        _remove_if_exists(client_log_local("vanilla"))
        if remote_client:
            _remote_rm(client_log_arg("vanilla"))
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
                [
                    str(client_bin) if not remote_client else str(args.remote_client_bin),
                    args.server_ip,
                    str(port),
                    client_log_arg("vanilla"),
                    str(args.vanilla_ngroups),
                    str(args.nclients),
                    str(args.nsets_exp),
                    str(args.ngets_exp),
                    str(vanilla_rps),
                ]
            ),
            run_log("vanilla"),
            args.timeout_sec,
        )
        if remote_client:
            _remote_fetch(client_log_arg("vanilla"), client_log_local("vanilla"))

        # sei
        port = _pick_ports(args.sei_ngroups)
        _remove_if_exists(client_log_local("sei"))
        if remote_client:
            _remote_rm(client_log_arg("sei"))
        _run_case(
            "sei",
            [
                _cmd_with_taskset(
                    cpus.server4,
                    [str(bins[sei_bin_key]), str(port), str(args.sei_ngroups)],
                    args.pin,
                )
            ],
            _client_cmd(
                [
                    str(client_bin) if not remote_client else str(args.remote_client_bin),
                    args.server_ip,
                    str(port),
                    client_log_arg("sei"),
                    str(args.sei_ngroups),
                    str(args.nclients),
                    str(args.nsets_exp),
                    str(args.ngets_exp),
                    str(sei_rps),
                ]
            ),
            run_log("sei"),
            args.timeout_sec,
        )
        if remote_client:
            _remote_fetch(client_log_arg("sei"), client_log_local("sei"))

        # orthrus
        port = _pick_ports(args.orthrus_ngroups)
        _remove_if_exists(client_log_local("orthrus"))
        if remote_client:
            _remote_rm(client_log_arg("orthrus"))
        orthrus_server_cmd: List[str] = [
            str(bins["orthrus"]),
            str(port),
            str(args.orthrus_ngroups),
        ]
        if args.preset == "fair4c":
            orthrus_server_cmd = [
                "env",
                f"SCEE_WORK_CPUSET={_format_cpu_list(orthrus_work_cpus)}",
                f"SCEE_VALIDATION_CPUSET={_format_cpu_list(orthrus_val_cpus)}",
                *orthrus_server_cmd,
            ]
        _run_case(
            "orthrus",
            [
                _cmd_with_taskset(
                    cpus.server8,
                    orthrus_server_cmd,
                    args.pin,
                )
            ],
            _client_cmd(
                [
                    str(client_bin) if not remote_client else str(args.remote_client_bin),
                    args.server_ip,
                    str(port),
                    client_log_arg("orthrus"),
                    str(args.orthrus_ngroups),
                    str(args.nclients),
                    str(args.nsets_exp),
                    str(args.ngets_exp),
                    str(orthrus_rps),
                ]
            ),
            run_log("orthrus"),
            args.timeout_sec,
        )
        if remote_client:
            _remote_fetch(client_log_arg("orthrus"), client_log_local("orthrus"))

        if args.orthrus_sync:
            # Orthrus (sync validation)
            port = _pick_ports(args.orthrus_ngroups)
            _remove_if_exists(client_log_local("orthrus_sync"))
            if remote_client:
                _remote_rm(client_log_arg("orthrus_sync"))
            orthrus_sync_server_cmd: List[str] = [
                str(bins["orthrus_sync"]),
                str(port),
                str(args.orthrus_ngroups),
            ]
            if args.preset == "fair4c":
                orthrus_sync_server_cmd = [
                    "env",
                    f"SCEE_WORK_CPUSET={_format_cpu_list(orthrus_work_cpus)}",
                    f"SCEE_VALIDATION_CPUSET={_format_cpu_list(orthrus_val_cpus)}",
                    *orthrus_sync_server_cmd,
                ]
            _run_case(
                "orthrus_sync",
                [
                    _cmd_with_taskset(
                        cpus.server8,
                        orthrus_sync_server_cmd,
                        args.pin,
                    )
                ],
                _client_cmd(
                    [
                        str(client_bin) if not remote_client else str(args.remote_client_bin),
                        args.server_ip,
                        str(port),
                        client_log_arg("orthrus_sync"),
                        str(args.orthrus_ngroups),
                        str(args.nclients),
                        str(args.nsets_exp),
                        str(args.ngets_exp),
                        str(orthrus_rps),
                    ]
                ),
                run_log("orthrus_sync"),
                args.timeout_sec,
            )
            if remote_client:
                _remote_fetch(
                    client_log_arg("orthrus_sync"), client_log_local("orthrus_sync")
                )

        # rbv
        replica_port = _pick_ports(args.rbv_ngroups)
        while True:
            port = _pick_ports(args.rbv_ngroups)
            if (
                port + args.rbv_ngroups - 1 < replica_port
                or replica_port + args.rbv_ngroups - 1 < port
            ):
                break
        _remove_if_exists(client_log_local("rbv"))
        if remote_client:
            _remote_rm(client_log_arg("rbv"))
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
                    [
                        str(bins["rbv_primary"]),
                        str(port),
                        str(args.rbv_ngroups),
                        str(replica_port),
                        "127.0.0.1",
                    ],
                    args.pin,
                ),
            ],
            _client_cmd(
                [
                    str(client_bin) if not remote_client else str(args.remote_client_bin),
                    args.server_ip,
                    str(port),
                    client_log_arg("rbv"),
                    str(args.rbv_ngroups),
                    str(args.nclients),
                    str(args.nsets_exp),
                    str(args.ngets_exp),
                    str(rbv_rps),
                ]
            ),
            run_log("rbv"),
            args.timeout_sec,
            server_start_interval_sec=2.0,
        )
        if remote_client:
            _remote_fetch(client_log_arg("rbv"), client_log_local("rbv"))

        subprocess.run(
            [
                sys.executable,
                str(root / "scripts" / "memcached" / "parse-throughput.py"),
                "--input-raw",
                str(client_log_local("vanilla")),
                "--input-sei",
                str(client_log_local("sei")),
                "--input-scee",
                str(client_log_local("orthrus")),
                *(
                    ["--input-scee-sync", str(client_log_local("orthrus_sync"))]
                    if args.orthrus_sync
                    else []
                ),
                "--input-rbv",
                str(client_log_local("rbv")),
                "-o",
                str(out_txt),
            ],
            check=True,
            cwd=str(root),
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

        def mem_client_log(name: str) -> Path:
            return temp_dir / f"memcached-mem-client-{name}{suffix}.log"

        def mem_client_log_arg(name: str) -> str:
            if not remote_client:
                return str(mem_client_log(name))
            return str(Path(args.client_temp_dir) / f"memcached-mem-client-{name}{suffix}.log")

        def run_log(name: str) -> Path:
            return temp_dir / f"run-memcached-mem-{name}{suffix}.log"

        def mem_status_log(name: str) -> Path:
            return temp_dir / f"memcached-memory_status-{name}{suffix}.log"

        # Ensure old logs don't get appended to (client opens output in append mode).
        names = ["vanilla", "sei", "orthrus", "rbv"]
        if args.orthrus_sync:
            names.append("orthrus_sync")
        for name in names:
            _remove_if_exists(mem_client_log(name))
            if remote_client:
                _remote_rm(mem_client_log_arg(name))

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
                [
                    str(client_bin) if not remote_client else str(args.remote_client_bin),
                    args.server_ip,
                    str(port),
                    mem_client_log_arg("vanilla"),
                    str(args.vanilla_ngroups),
                    str(args.nclients),
                    str(args.nsets_exp),
                    str(args.ngets_exp),
                    str(vanilla_rps),
                ]
            ),
            run_log("vanilla"),
            args.timeout_sec,
        )
        if remote_client:
            _remote_fetch(mem_client_log_arg("vanilla"), mem_client_log("vanilla"))
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
                [
                    str(client_bin) if not remote_client else str(args.remote_client_bin),
                    args.server_ip,
                    str(port),
                    mem_client_log_arg("sei"),
                    str(args.sei_ngroups),
                    str(args.nclients),
                    str(args.nsets_exp),
                    str(args.ngets_exp),
                    str(sei_rps),
                ]
            ),
            run_log("sei"),
            args.timeout_sec,
        )
        if remote_client:
            _remote_fetch(mem_client_log_arg("sei"), mem_client_log("sei"))
        move_if_exists(
            root / "memcached-memory_status-sei.log",
            mem_status_log("sei"),
        )

        # orthrus
        port = _pick_ports(args.orthrus_ngroups)
        _remove_if_exists(root / "memcached-memory_status-orthrus.log")
        orthrus_mem_server_cmd: List[str] = [
            str(bins["orthrus_mem"]),
            str(port),
            str(args.orthrus_ngroups),
        ]
        if args.preset == "fair4c":
            orthrus_mem_server_cmd = [
                "env",
                f"SCEE_WORK_CPUSET={_format_cpu_list(orthrus_work_cpus)}",
                f"SCEE_VALIDATION_CPUSET={_format_cpu_list(orthrus_val_cpus)}",
                *orthrus_mem_server_cmd,
            ]
        _run_case(
            "orthrus_mem",
            [
                _cmd_with_taskset(
                    cpus.server8,
                    orthrus_mem_server_cmd,
                    args.pin,
                )
            ],
            _client_cmd(
                [
                    str(client_bin) if not remote_client else str(args.remote_client_bin),
                    args.server_ip,
                    str(port),
                    mem_client_log_arg("orthrus"),
                    str(args.orthrus_ngroups),
                    str(args.nclients),
                    str(args.nsets_exp),
                    str(args.ngets_exp),
                    str(orthrus_rps),
                ]
            ),
            run_log("orthrus"),
            args.timeout_sec,
        )
        if remote_client:
            _remote_fetch(mem_client_log_arg("orthrus"), mem_client_log("orthrus"))
        move_if_exists(
            root / "memcached-memory_status-orthrus.log",
            mem_status_log("orthrus"),
        )

        if args.orthrus_sync:
            # orthrus (sync validation)
            port = _pick_ports(args.orthrus_ngroups)
            _remove_if_exists(root / "memcached-memory_status-orthrus.log")
            orthrus_sync_mem_server_cmd: List[str] = [
                str(bins["orthrus_sync_mem"]),
                str(port),
                str(args.orthrus_ngroups),
            ]
            if args.preset == "fair4c":
                orthrus_sync_mem_server_cmd = [
                    "env",
                    f"SCEE_WORK_CPUSET={_format_cpu_list(orthrus_work_cpus)}",
                    f"SCEE_VALIDATION_CPUSET={_format_cpu_list(orthrus_val_cpus)}",
                    *orthrus_sync_mem_server_cmd,
                ]
            _run_case(
                "orthrus_sync_mem",
                [
                    _cmd_with_taskset(
                        cpus.server8,
                        orthrus_sync_mem_server_cmd,
                        args.pin,
                    )
                ],
                _client_cmd(
                    [
                        str(client_bin) if not remote_client else str(args.remote_client_bin),
                        args.server_ip,
                        str(port),
                        mem_client_log_arg("orthrus_sync"),
                        str(args.orthrus_ngroups),
                        str(args.nclients),
                        str(args.nsets_exp),
                        str(args.ngets_exp),
                        str(orthrus_rps),
                    ]
                ),
                run_log("orthrus_sync"),
                args.timeout_sec,
            )
            if remote_client:
                _remote_fetch(
                    mem_client_log_arg("orthrus_sync"), mem_client_log("orthrus_sync")
                )
            move_if_exists(
                root / "memcached-memory_status-orthrus.log",
                mem_status_log("orthrus_sync"),
            )

        # rbv
        replica_port = _pick_ports(args.rbv_ngroups)
        while True:
            port = _pick_ports(args.rbv_ngroups)
            if (
                port + args.rbv_ngroups - 1 < replica_port
                or replica_port + args.rbv_ngroups - 1 < port
            ):
                break
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
                    [
                        str(bins["rbv_primary_mem"]),
                        str(port),
                        str(args.rbv_ngroups),
                        str(replica_port),
                        "127.0.0.1",
                    ],
                    args.pin,
                ),
            ],
            _client_cmd(
                [
                    str(client_bin) if not remote_client else str(args.remote_client_bin),
                    args.server_ip,
                    str(port),
                    mem_client_log_arg("rbv"),
                    str(args.rbv_ngroups),
                    str(args.nclients),
                    str(args.nsets_exp),
                    str(args.ngets_exp),
                    str(rbv_rps),
                ]
            ),
            run_log("rbv"),
            args.timeout_sec,
            server_start_interval_sec=2.0,
        )
        if remote_client:
            _remote_fetch(mem_client_log_arg("rbv"), mem_client_log("rbv"))
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
