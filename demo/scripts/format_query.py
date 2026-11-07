"""Print a Weir /api/query result (JSON on stdin) as plain text for score.sh."""

import json
import sys


def table(columns, rows):
    cells = [[str(c) for c in columns]] + [["-" if v is None else str(v) for v in row] for row in rows]
    widths = [max(len(r[i]) for r in cells) for i in range(len(columns))]
    for r in cells:
        print("  " + "  ".join(v.ljust(w) for v, w in zip(r, widths)))


def main():
    res = json.load(sys.stdin)
    if "error" in res:
        print("  error:", res["error"])
    elif res["type"] == "table":
        table(res["columns"], res.get("rows") or [])
    elif res["type"] == "series":
        rows = []
        for s in res.get("series") or []:
            values = [v for _, v in s["points"] if v is not None]
            rows.append([s["name"], values[-1] if values else None, max(values) if values else None])
        table(["series", f"last ({res.get('unit', '')})", "max"], rows)
    elif res["type"] == "graph":
        keys = [
            "stage",
            "status",
            "in",
            "emitted",
            "vanished",
            "errored",
            "retried",
            "in_flight",
            "unaccounted",
            "oldest_age",
        ]
        table(keys, [[n.get(k) for k in keys] for n in res.get("nodes") or []])


if __name__ == "__main__":
    main()
