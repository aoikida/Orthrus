import json
import argparse

from utils import parse


parser = argparse.ArgumentParser()
parser.add_argument("--input-raw", required=True)
parser.add_argument("--input-sei", required=True)
parser.add_argument("--input-scee", required=False, help="Orthrus (async validation) client log.")
parser.add_argument(
    "--input-scee-sync",
    required=False,
    help="Orthrus (sync validation; SCEE_SYNC_VALIDATE) client log.",
)
parser.add_argument("--input-rbv", required=True)
parser.add_argument("-o", "--output", required=True)

args = parser.parse_args()

if args.input_scee is None and args.input_scee_sync is None:
    raise SystemExit("parse-throughput.py: require --input-scee and/or --input-scee-sync")

raw = parse(args.input_raw)
assert len(raw) == 1

sei = parse(args.input_sei)
assert len(sei) == 1

scee = None
if args.input_scee is not None:
    scee = parse(args.input_scee)
    assert len(scee) == 1

scee_sync = None
if args.input_scee_sync is not None:
    scee_sync = parse(args.input_scee_sync)
    assert len(scee_sync) == 1

rbv = parse(args.input_rbv)
assert len(rbv) == 1

with open(args.output, "w") as fout:
    fout.write("vanilla running\n")
    fout.write(f"throughput: {raw[0]['throughput']}\n")
    fout.write("sei running\n")
    fout.write(f"throughput: {sei[0]['throughput']}\n")
    if scee is not None:
        fout.write("orthrus running\n")
        fout.write(f"throughput: {scee[0]['throughput']}\n")
    if scee_sync is not None:
        fout.write("orthrus_sync running\n")
        fout.write(f"throughput: {scee_sync[0]['throughput']}\n")
    fout.write("rbv running\n")
    fout.write(f"throughput: {rbv[0]['throughput']}\n")

with open(f"{args.output}.json", "w") as fout:
    data = {
        "vanilla": raw[0],
        "sei": sei[0],
        "rbv": rbv[0],
    }
    if scee is not None:
        data["orthrus"] = scee[0]
    if scee_sync is not None:
        data["orthrus_sync"] = scee_sync[0]
    json.dump(data, fout, ensure_ascii=False, indent=2)
