"""Read a Delta transaction log with plain Python: commits, live files, data skipping."""

import argparse
import json
from pathlib import Path


def load_commits(log: Path):
    for commit in sorted(log.glob("*.json")):
        lines = [ln for ln in commit.read_text().splitlines() if ln.strip()]
        yield commit.name, [json.loads(ln) for ln in lines]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("path")
    p.add_argument("--cell", type=int, default=None)
    args = p.parse_args()

    live: dict[str, dict] = {}
    for name, actions in load_commits(Path(args.path) / "_delta_log"):
        kinds: dict[str, int] = {}
        for a in actions:
            k = next(iter(a))
            kinds[k] = kinds.get(k, 0) + 1
            if k == "add":
                live[a["add"]["path"]] = a["add"]
            elif k == "remove":
                live.pop(a["remove"]["path"], None)
        info = next((a["commitInfo"] for a in actions if "commitInfo" in a), {})
        print(f"{name}: {info.get('operation', '?'):<8} {kinds}  live after: {len(live)}")

    total = sum(f["size"] for f in live.values())
    print(f"\nlive: {len(live)} files, {total:,} B")
    if args.cell is not None:
        hit = 0
        for f in live.values():
            s = json.loads(f.get("stats") or "{}")
            lo = s.get("minValues", {}).get("cell_idx")
            hi = s.get("maxValues", {}).get("cell_idx")
            if lo is None or hi is None or lo <= args.cell <= hi:
                hit += 1
        print(f"files a query for cell_idx={args.cell} must read: {hit} of {len(live)}")


if __name__ == "__main__":
    main()