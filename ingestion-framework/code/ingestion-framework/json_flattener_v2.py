#!/usr/bin/env python3
"""
json_flattener_v2.2.py

Patch highlights over v2.1
--------------------------
- Full exception handling for:
  * File I/O errors (read/write)
  * JSON decode errors (input)
  * BrokenPipeError when streaming to stdout
  * KeyboardInterrupt (Ctrl+C) graceful exit
- CLI interface unchanged.

Core features (unchanged from v2.1)
-----------------------------------
- Scoped inheritance (no leakage)
- List-of-lists expansion
- Iterator mode (stream rows)
- Optional parent stub rows (--emit-empty-parent)
- Guards: --max-depth, --max-rows, --max-cols
- IDs & lineage: _row_id, _parent_id, _path (JSON Pointer), _elem_index, _depth
- Schema manifest (--schema-out)

pip install pandas numpy

"""

import argparse
import json
import itertools
import sys
from typing import Any, Dict, List, Optional, Tuple, Union
from collections import defaultdict

Scalar = Union[str, int, float, bool, None]
JSONType = Union[Scalar, Dict[str, Any], List[Any]]


# --------------- small helpers ---------------

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def is_scalar(x: Any) -> bool:
    return not isinstance(x, (dict, list))


def split_dict(d: Dict[str, Any]) -> Tuple[Dict[str, Scalar], Dict[str, Any]]:
    scalars, nested = {}, {}
    for k, v in d.items():
        (scalars if is_scalar(v) else nested)[k] = v
    return scalars, nested


def prefix_keys(d: Dict[str, Any], path: str, joiner: str) -> Dict[str, Any]:
    if not path:
        return dict(d)
    return {f"{path}{joiner}{k}": v for k, v in d.items()}


# --------------- schema manifest ---------------

class SchemaManifest:
    def __init__(self) -> None:
        self._cols = defaultdict(set)
        self._types = defaultdict(lambda: defaultdict(set))

    def observe(self, table: str, row: Dict[str, Any]) -> None:
        for k, v in row.items():
            self._cols[table].add(k)
            self._types[table][k].add(type(v).__name__)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for tbl, cols in self._cols.items():
            cols_sorted = sorted(cols)
            out[tbl] = {
                "columns": cols_sorted,
                "types": {c: sorted(self._types[tbl][c]) for c in cols_sorted},
            }
        return out


# --------------- guards ---------------

class RowGuard:
    def __init__(self, max_depth=None, max_rows=None, max_cols=None, verbose=True):
        self.max_depth = max_depth
        self.max_rows = max_rows
        self.max_cols = max_cols
        self.rows_emitted = 0
        self.verbose = verbose

    def check_depth(self, depth: int) -> bool:
        return self.max_depth is None or depth <= self.max_depth

    def can_emit_row(self) -> bool:
        return self.max_rows is None or self.rows_emitted < self.max_rows

    def note_emitted(self) -> None:
        self.rows_emitted += 1

    def trim_columns(self, row: Dict[str, Any]) -> Dict[str, Any]:
        if self.max_cols and len(row) > self.max_cols:
            # always preserve meta fields
            keep = {"tableName", "_row_id", "_parent_id", "_path", "_elem_index", "_depth"}
            keys = list(row.keys())
            # non-meta first
            non_meta = [k for k in keys if k not in keep]
            space_for_non_meta = max(0, self.max_cols - len(keep))
            trimmed_non_meta = non_meta[:space_for_non_meta]
            final_keys = trimmed_non_meta + [k for k in keys if k in keep]
            trimmed = {k: row[k] for k in final_keys if k in row}
            if self.verbose:
                eprint(f"[WARN] Row trimmed from {len(row)} to {len(trimmed)} columns")
            return trimmed
        return row


# --------------- flattener core ---------------

class FlattenerV2:
    def __init__(self, joiner="_", numeric_to_float=False, emit_empty_parent=False,
                 guard=None, schema=None):
        self.joiner = joiner
        self.numeric_to_float = numeric_to_float
        self.emit_empty_parent = emit_empty_parent
        self.guard = guard or RowGuard()
        self.schema = schema or SchemaManifest()
        self._row_id_counter = itertools.count(1)
        self._parent_stack: List[int] = []

    def iter_rows(self, data: JSONType):
        yield from self._walk(data, path="", jsonptr="", depth=0, inherited={})

    # traversal
    def _walk(self, node, path, jsonptr, depth, inherited):
        if not self.guard.check_depth(depth):
            return
        if isinstance(node, dict):
            scalars, nested = split_dict(node)
            inherited_here = {**inherited, **prefix_keys(scalars, path, self.joiner)}
            for k, v in nested.items():
                child_path = f"{path}{self.joiner}{k}" if path else k
                child_ptr = f"{jsonptr}/{k}"
                if isinstance(v, dict):
                    yield from self._walk(v, child_path, child_ptr, depth + 1, inherited_here)
                elif isinstance(v, list):
                    yield from self._expand_list(v, child_path, child_ptr, depth + 1, inherited_here)
        elif isinstance(node, list):
            yield from self._expand_list(node, path, jsonptr, depth + 1, inherited)
        # scalars produce no rows here

    def _expand_list(self, arr, path, jsonptr, depth, inherited):
        for i, el in enumerate(arr):
            elem_ptr = f"{jsonptr}/{i}"
            if is_scalar(el):
                row = self._make_row(path, elem_ptr, inherited, i, depth)
                row[path] = self._maybe_float(el)
                yield from self._emit_row(row)
            elif isinstance(el, dict):
                scalars, nested = split_dict(el)
                prefixed = prefix_keys(scalars, path, self.joiner)
                has_children = any(isinstance(v, (list, dict)) for v in nested.values())
                parent_pushed = False
                if prefixed or (self.emit_empty_parent and has_children):
                    row = self._make_row(path, elem_ptr, {**inherited, **prefixed}, i, depth)
                    row.update({k: self._maybe_float(v) for k, v in prefixed.items()})
                    for out in self._emit_row(row):
                        self._parent_stack.append(out["_row_id"])
                        parent_pushed = True
                        yield out
                next_inherited = {**inherited, **prefixed}
                for k, v in nested.items():
                    child_path = f"{path}{self.joiner}{k}"
                    child_ptr = f"{elem_ptr}/{k}"
                    if isinstance(v, list):
                        yield from self._expand_list(v, child_path, child_ptr, depth + 1, next_inherited)
                    elif isinstance(v, dict):
                        yield from self._walk(v, child_path, child_ptr, depth + 1, next_inherited)
                if parent_pushed and self._parent_stack:
                    self._parent_stack.pop()
            elif isinstance(el, list):
                # list-of-lists
                yield from self._expand_list(el, path, elem_ptr, depth + 1, inherited)

    # row helpers
    def _make_row(self, table, jsonptr, inherited, elem_index, depth):
        row = dict(inherited)
        row["tableName"] = table
        row["_elem_index"] = elem_index
        row["_depth"] = depth
        row["_row_id"] = next(self._row_id_counter)
        row["_parent_id"] = self._parent_stack[-1] if self._parent_stack else None
        row["_path"] = jsonptr or "/"
        return row

    def _emit_row(self, row):
        if not self.guard.can_emit_row():
            return
        row = self.guard.trim_columns(row)
        self.schema.observe(row["tableName"], row)
        self.guard.note_emitted()
        yield row

    def _maybe_float(self, v):
        return float(v) if self.numeric_to_float and isinstance(v, int) and not isinstance(v, bool) else v


# --------------- safe I/O helpers ---------------

def safe_load_json(infile: Optional[str]) -> JSONType:
    try:
        if infile:
            with open(infile, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            return json.load(sys.stdin)
    except FileNotFoundError:
        eprint(f"[ERROR] Input file not found: {infile}")
        sys.exit(2)
    except PermissionError:
        eprint(f"[ERROR] Permission denied reading: {infile or 'stdin'}")
        sys.exit(2)
    except json.JSONDecodeError as e:
        eprint(f"[ERROR] Invalid JSON in {infile or 'stdin'}: {e}")
        sys.exit(2)
    except OSError as e:
        eprint(f"[ERROR] OS error reading {infile or 'stdin'}: {e}")
        sys.exit(2)


def safe_dump_json_array(rows_iter, outfile: Optional[str]) -> None:
    """
    Materialize rows and write as a JSON array either to file or stdout.
    """
    try:
        rows = list(rows_iter)
        if outfile:
            with open(outfile, "w", encoding="utf-8") as f:
                json.dump(rows, f, indent=2, ensure_ascii=False)
        else:
            json.dump(rows, sys.stdout, indent=2, ensure_ascii=False)
            sys.stdout.write("\n")
    except BrokenPipeError:
        # Standard Unix behavior: exit quietly when consumer closes pipe
        try:
            sys.stdout.close()
        finally:
            sys.exit(0)
    except PermissionError:
        eprint(f"[ERROR] Permission denied writing: {outfile or 'stdout'}")
        sys.exit(3)
    except OSError as e:
        eprint(f"[ERROR] OS error writing {outfile or 'stdout'}: {e}")
        sys.exit(3)


def safe_dump_ndjson(rows_iter, outfile: Optional[str]) -> None:
    """
    Stream rows as NDJSON to stdout or write to file (line-per-row).
    """
    try:
        if outfile:
            with open(outfile, "w", encoding="utf-8") as f:
                for r in rows_iter:
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
        else:
            for r in rows_iter:
                sys.stdout.write(json.dumps(r, ensure_ascii=False) + "\n")
    except BrokenPipeError:
        try:
            sys.stdout.close()
        finally:
            sys.exit(0)
    except PermissionError:
        eprint(f"[ERROR] Permission denied writing: {outfile or 'stdout'}")
        sys.exit(3)
    except OSError as e:
        eprint(f"[ERROR] OS error writing {outfile or 'stdout'}: {e}")
        sys.exit(3)


def safe_dump_schema(schema: SchemaManifest, path: Optional[str]) -> None:
    if not path:
        return
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(schema.to_dict(), f, indent=2, ensure_ascii=False)
    except PermissionError:
        eprint(f"[ERROR] Permission denied writing schema: {path}")
        sys.exit(3)
    except OSError as e:
        eprint(f"[ERROR] OS error writing schema {path}: {e}")
        sys.exit(3)


# --------------- CLI ---------------

def build_argparser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Flatten nested JSON (arrays -> rows) with streaming and safeguards.")
    ap.add_argument("--in", dest="infile", help="Input JSON file path. If omitted, read from stdin.")
    ap.add_argument("--out", dest="outfile", help="Output JSON file path (array). If omitted, write to stdout.")
    ap.add_argument("--ndjson", action="store_true", help="Stream rows as NDJSON to stdout (ignored if --out is used).")

    ap.add_argument("--numeric-to-float", action="store_true", help="Cast ints to floats (booleans preserved).")
    ap.add_argument("--emit-empty-parent", action="store_true", help="Emit parent stub rows for dict elements with no scalars but with nested arrays.")

    ap.add_argument("--max-depth", type=int, help="Maximum traversal depth (guard).")
    ap.add_argument("--max-rows", type=int, help="Maximum rows to emit (guard).")
    ap.add_argument("--max-cols", type=int, help="Maximum columns per row (guard).")

    ap.add_argument("--schema-out", type=str, help="Write schema manifest JSON to this path.")
    return ap


def main():
    ap = build_argparser()
    try:
        args = ap.parse_args()

        data = safe_load_json(args.infile)

        guard = RowGuard(args.max_depth, args.max_rows, args.max_cols)
        schema = SchemaManifest()
        fl = FlattenerV2(
            numeric_to_float=args.numeric_to_float,
            emit_empty_parent=args.emit_empty_parent,
            guard=guard,
            schema=schema,
        )

        rows_iter = fl.iter_rows(data)

        # Output
        if args.outfile:
            # Force JSON array to file
            safe_dump_json_array(rows_iter, args.outfile)
        else:
            # Stdout: ndjson if requested, else JSON array
            if args.ndjson:
                safe_dump_ndjson(rows_iter, None)
            else:
                safe_dump_json_array(rows_iter, None)

        # Schema
        safe_dump_schema(schema, args.schema_out)

    except KeyboardInterrupt:
        eprint("Aborted by user.")
        sys.exit(130)  # 128 + SIGINT
    except SystemExit:
        # Let argparse/sys.exit propagate cleanly
        raise
    except Exception as e:
        # Last-resort catch: unexpected runtime errors
        eprint(f"[ERROR] Unexpected error: {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
