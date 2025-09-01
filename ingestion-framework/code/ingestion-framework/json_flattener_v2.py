#!/usr/bin/env python3
"""
json_flattener_v2.1.py

Patched improvements over v2:
- Fixed scalar inheritance bug (typo).
- Cleaned list-of-lists handling (no noisy numeric indices in paths).
- `_path` now carries JSON-pointer style lineage, not just tableName.
- Added optional warning when columns are trimmed by guards.
- Booleans never cast to float when --numeric-to-float.

pip install pandas pyarrow

"""

import argparse, json, itertools, sys
from typing import Any, Dict, List, Optional, Tuple, Union
from collections import defaultdict, OrderedDict

Scalar = Union[str, int, float, bool, None]
JSONType = Union[Scalar, Dict[str, Any], List[Any]]


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


class SchemaManifest:
    def __init__(self) -> None:
        self._cols = defaultdict(set)
        self._types = defaultdict(lambda: defaultdict(set))

    def observe(self, table: str, row: Dict[str, Any]) -> None:
        for k, v in row.items():
            self._cols[table].add(k)
            self._types[table][k].add(type(v).__name__)

    def to_dict(self) -> Dict[str, Any]:
        return {
            tbl: {
                "columns": sorted(cols),
                "types": {c: sorted(self._types[tbl][c]) for c in cols}
            }
            for tbl, cols in self._cols.items()
        }


class RowGuard:
    def __init__(self, max_depth=None, max_rows=None, max_cols=None, verbose=True):
        self.max_depth, self.max_rows, self.max_cols = max_depth, max_rows, max_cols
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
            # Always preserve meta
            keep = {"tableName","_row_id","_parent_id","_path","_elem_index","_depth"}
            keys = list(row.keys())
            core = [k for k in keys if k not in keep][:self.max_cols - len(keep)]
            trimmed = {k: row[k] for k in core + list(keep) if k in row}
            if self.verbose:
                sys.stderr.write(f"[WARN] Row trimmed from {len(row)} to {len(trimmed)} cols\n")
            return trimmed
        return row


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

    def iter_rows(self, data: JSONType) -> List[Dict[str, Any]]:
        yield from self._walk(data, "", "", 0, {})

    # ---------- traversal ----------
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
                    yield from self._walk(v, child_path, child_ptr, depth+1, inherited_here)
                elif isinstance(v, list):
                    yield from self._expand_list(v, child_path, child_ptr, depth+1, inherited_here)
        elif isinstance(node, list):
            yield from self._expand_list(node, path, jsonptr, depth+1, inherited)

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
                has_children = any(isinstance(v,(list,dict)) for v in nested.values())
                if prefixed or (self.emit_empty_parent and has_children):
                    row = self._make_row(path, elem_ptr, {**inherited, **prefixed}, i, depth)
                    row.update({k: self._maybe_float(v) for k,v in prefixed.items()})
                    for out in self._emit_row(row):
                        parent_id = out["_row_id"]
                        self._parent_stack.append(parent_id)
                        yield out
                for k,v in nested.items():
                    child_path = f"{path}{self.joiner}{k}"
                    child_ptr = f"{elem_ptr}/{k}"
                    if isinstance(v, list):
                        yield from self._expand_list(v, child_path, child_ptr, depth+1, {**inherited, **prefixed})
                    elif isinstance(v, dict):
                        yield from self._walk(v, child_path, child_ptr, depth+1, {**inherited, **prefixed})
                if self._parent_stack: self._parent_stack.pop()
            elif isinstance(el, list):
                # nested list-of-lists
                yield from self._expand_list(el, path, elem_ptr, depth+1, inherited)

    # ---------- helpers ----------
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

    def _maybe_float(self,v):
        return float(v) if self.numeric_to_float and isinstance(v,int) and not isinstance(v,bool) else v


# ---------------- CLI ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in",dest="infile")
    ap.add_argument("--out",dest="outfile")
    ap.add_argument("--ndjson",action="store_true")
    ap.add_argument("--numeric-to-float",action="store_true")
    ap.add_argument("--emit-empty-parent",action="store_true")
    ap.add_argument("--max-depth",type=int)
    ap.add_argument("--max-rows",type=int)
    ap.add_argument("--max-cols",type=int)
    ap.add_argument("--schema-out")
    args=ap.parse_args()

    data = json.load(open(args.infile)) if args.infile else json.load(sys.stdin)
    guard = RowGuard(args.max_depth,args.max_rows,args.max_cols)
    schema = SchemaManifest()
    fl = FlattenerV2(numeric_to_float=args.numeric_to_float,
                     emit_empty_parent=args.emit_empty_parent,
                     guard=guard,schema=schema)
    rows = fl.iter_rows(data)

    if args.outfile:
        json.dump(list(rows),open(args.outfile,"w"),indent=2,ensure_ascii=False)
    elif args.ndjson:
        for r in rows: print(json.dumps(r,ensure_ascii=False))
    else:
        print(json.dumps(list(rows),indent=2,ensure_ascii=False))

    if args.schema_out:
        json.dump(schema.to_dict(),open(args.schema_out,"w"),indent=2,ensure_ascii=False)


if __name__=="__main__": main()
