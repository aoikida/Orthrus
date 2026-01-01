import re
import argparse
from typing import List
from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument("--input-raw", required=True, help="lsmtree-memory_status-raw.log")
parser.add_argument("--input-sei", required=False, help="lsmtree-memory_status-sei.log")
parser.add_argument("--input-scee", required=True, help="lsmtree-memory_status-scee.log")
parser.add_argument(
    "--input-scee-sync",
    required=False,
    help="lsmtree-memory_status-scee-sync.log",
)
parser.add_argument(
    "--input-rbv",
    required=True,
    action="append",
    help="lsmtree-memory_status-rbv.log",
)

args = parser.parse_args()

raw_mem = Path(args.input_raw)
sei_mem = Path(args.input_sei) if args.input_sei else None
scee_mem = Path(args.input_scee)
scee_sync_mem = Path(args.input_scee_sync) if args.input_scee_sync else None

rbv_mems = [Path(x) for x in args.input_rbv]


def parser(mem: Path):
    pat = re.compile(r"VmRSS:\s+(\d+) kB")

    with open(mem, "r", encoding="utf8") as f:
        data: List[str] = f.read().splitlines()

    run_stage_parsed: List[int] = []
    for line in data:
        matches = pat.match(line)
        if not matches:
            continue
        run_stage_parsed.append(int(matches[1]))

    if not run_stage_parsed:
        raise ValueError(f"No VmRSS samples found in {mem}")
    max_run_stage_mem = max(run_stage_parsed)
    avg_run_stage_mem = sum(run_stage_parsed) // len(run_stage_parsed)

    print("max mem run : ", max_run_stage_mem)
    return (
        max_run_stage_mem,
        avg_run_stage_mem,
    )


print("Processing raw")
raw_max_run_mem, raw_avg_run_mem = parser(raw_mem)

sei_max_run_mem = None
sei_avg_run_mem = None
if sei_mem is not None:
    print("Processing sei")
    sei_max_run_mem, sei_avg_run_mem = parser(sei_mem)

print("Processing scee")
scee_max_run_mem, scee_avg_run_mem = parser(scee_mem)

scee_sync_max_run_mem = None
scee_sync_avg_run_mem = None
if scee_sync_mem is not None:
    print("Processing scee(sync)")
    scee_sync_max_run_mem, scee_sync_avg_run_mem = parser(scee_sync_mem)

print("Processing rbv")
rbv_max_run_mem = 0
rbv_avg_run_mem = 0
for rbv_mem in rbv_mems:
    _rbv_max_run_mem, _rbv_avg_run_mem = parser(rbv_mem)
    rbv_max_run_mem += _rbv_max_run_mem
    rbv_avg_run_mem += _rbv_avg_run_mem


diff = lambda a, b: a / b

print("-" * 10, " results(peak) ", "-" * 10)
print("ratio (Orthrus(async) vs Vanilla): ", diff(scee_max_run_mem, raw_max_run_mem))
if scee_sync_max_run_mem is not None:
    print(
        "ratio (Orthrus(sync) vs Vanilla):  ",
        diff(scee_sync_max_run_mem, raw_max_run_mem),
    )
if sei_max_run_mem is not None:
    print("ratio (SEI vs Vanilla):     ", diff(sei_max_run_mem, raw_max_run_mem))
print("ratio (RBV vs Vanilla):     ", diff(rbv_max_run_mem, raw_max_run_mem))

print("-" * 10, " results(avg) ", "-" * 10)
print("ratio (Orthrus(async) vs Vanilla): ", diff(scee_avg_run_mem, raw_avg_run_mem))
if scee_sync_avg_run_mem is not None:
    print(
        "ratio (Orthrus(sync) vs Vanilla):  ",
        diff(scee_sync_avg_run_mem, raw_avg_run_mem),
    )
if sei_avg_run_mem is not None:
    print("ratio (SEI vs Vanilla):     ", diff(sei_avg_run_mem, raw_avg_run_mem))
print("ratio (RBV vs Vanilla):     ", diff(rbv_avg_run_mem, raw_avg_run_mem))
