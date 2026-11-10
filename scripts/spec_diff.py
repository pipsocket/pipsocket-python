"""Compare two copies of the OpenAPI document, as parsed JSON (never as bytes).

    python scripts/spec_diff.py OLD NEW [--report FILE]   exit 0 if the same, 1 if not
    python scripts/spec_diff.py --fingerprint FILE         print the document's fingerprint

The report covers what a general OpenAPI diff doesn't understand: the `x-pipsocket` extension's
error codes and retcodes. The published document is minified and writes whole numbers without a
decimal point, so comparing bytes would report changes that aren't there.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

Json = Any


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("documents", nargs="*", type=Path)
    parser.add_argument("--report", type=Path, help="write a Markdown report here")
    parser.add_argument("--fingerprint", type=Path, help="print the document's fingerprint")
    args = parser.parse_args()

    if args.fingerprint:
        print(fingerprint(load(args.fingerprint)))
        return 0
    if len(args.documents) != 2:
        parser.error("give OLD and NEW")

    old, new = (load(path) for path in args.documents)
    lines = report(old, new)
    text = "\n".join(lines) + "\n"
    if args.report:
        args.report.write_text(text)
    print(text, end="")
    return 0 if old == new else 1


def load(path: Path) -> Json:
    return json.loads(path.read_text(encoding="utf-8"))


def fingerprint(document: Json) -> str:
    """A short hash of the parsed document, the same however it is formatted."""
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode()).hexdigest()[:12]


def report(old: Json, new: Json) -> list[str]:
    if old == new:
        return [f"The document is unchanged ({fingerprint(new)})."]
    lines = [f"The document changed: {fingerprint(old)} to {fingerprint(new)}.", ""]
    lines += section("Error codes", by_key(old, "errors", "code"), by_key(new, "errors", "code"))
    lines += section("Retcodes", by_key(old, "retcodes", "code"), by_key(new, "retcodes", "code"))
    extension_old, extension_new = old.get("x-pipsocket", {}), new.get("x-pipsocket", {})
    others = sorted(
        key
        for key in set(extension_old) | set(extension_new)
        if key not in ("errors", "retcodes") and extension_old.get(key) != extension_new.get(key)
    )
    if others:
        lines += ["### Other x-pipsocket entries", "", "Changed: " + ", ".join(others), ""]
    return lines


def by_key(document: Json, table: str, key: str) -> dict[str, Json]:
    entries = document.get("x-pipsocket", {}).get(table, [])
    return {str(entry[key]): entry for entry in entries}


def section(title: str, old: dict[str, Json], new: dict[str, Json]) -> list[str]:
    added = sorted(set(new) - set(old))
    removed = sorted(set(old) - set(new))
    changed = sorted(key for key in set(old) & set(new) if old[key] != new[key])
    if not (added or removed or changed):
        return []
    lines = [f"### {title}", ""]
    lines += [f"- added `{key}`: {describe(new[key])}" for key in added]
    lines += [f"- removed `{key}`" for key in removed]
    for key in changed:
        before, after = old[key], new[key]
        fields = sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))
        moves = ", ".join(f"{f} {before.get(f)!r} to {after.get(f)!r}" for f in fields)
        lines.append(f"- changed `{key}`: {moves}")
    return [*lines, ""]


def describe(entry: Json) -> str:
    return ", ".join(f"{k} {v!r}" for k, v in entry.items() if k != "code")


if __name__ == "__main__":
    sys.exit(main())
