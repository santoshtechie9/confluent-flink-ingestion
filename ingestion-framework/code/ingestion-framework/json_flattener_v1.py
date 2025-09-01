#!/usr/bin/env python3
"""
json_flattener_v1.py

Generic JSON flattener that expands multiple nested arrays into a table-like
list of row dicts. It emits one row per array element (including arrays nested
inside array elements) and carries parent/context fields into each row.

v1 Improvements
---------------
1) Preserve booleans when casting numbers:
   - Fixed a bug where bools could be coerced to floats (Python bool is a subclass of int).

2) Mixed lists supported:
   - Lists containing both scalars and dicts now emit rows for scalar elements too
     (previously, non-dict items in a mixed list were skipped).

3) Clearer deep-dict inheritance:
   - When a list element contains nested dicts with scalar fields, those scalars
     are added to the inherited context cleanly before recursing.

4) Safer, cleaner internals:
   - Tidied logic, removed obfuscation, and kept behavior identical to the prior
     design for array-of-dicts “parent rows” (e.g., `product_tsf` rows with only
     `businessDateAdded`) plus nested array rows (e.g., `product_tsf_tradeSourceId`).

Key behavior (unchanged from your intent)
-----------------------------------------
- Dict-only paths accumulate scalars as inherited context (e.g., `product_code`,
  `entId_id`, etc.) and are present on every emitted row by default.
- Arrays:
  * Arrays of scalars -> one row per element (`tableName` = path).
  * Arrays of dicts -> emit a “parent row” when the element has scalars (`tableName` = path),
    then expand any nested arrays beneath it (carrying inherited + element scalars).
- `tableName` is the underscore-joined JSON path.
- Optional `--numeric-to-float` to match schemas that want ints as floats.

Usage
-----
python json_flattener_v1.py --in event.json --out rows.json --inherit all --numeric-to-float
"""

from __future__ import annotations
import argparse
import copy
import json
from typing import Any, Dict, List, Tuple, Union, Optional

Scalar = Union[str, int, float, bool, None]
JSONType = Union[Scalar, Dict[str, Any], List[Any]]


# ------------------------- Small utilities -------------------------

def is_scalar(x: Any) -> bool:
    return not isinstance(x, (dict, list))


def prefix_keys(d: Dict[str, Any], path: str, joiner: str = "_") -> Dict[str, Any]:
    """Prefix a dict's keys with a path using joiner (e.g., 'product' + '_' + 'code')."""
    if not path:
        return dict(d)
    return {f"{path}{joiner}{k}": v for k, v in d.items()}


def split_dict(d: Dict[str, Any]) -> Tuple[Dict[str, Scalar], Dict[str, Any]]:
    """
    Split a dict into:
      - scalars_only: {k: v} for non-dict/non-list values
      - nested: {k: v} for dicts/lists
    """
    scalars_only: Dict[str, Scalar] = {}
    nested: Dict[str, Any] = {}
    for k, v in d.items():
        if is_scalar(v):
            scalars_only[k] = v
        else:
            nested[k] = v
    return scalars_only, nested


def coerce_numbers_to_float(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Optional: cast ints to floats to match schemas like your example.
    IMPORTANT: Do NOT convert booleans (bool is a subclass of int).
    """
    out: Dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, bool):          # keep True/False as-is
            out[k] = v
        elif isinstance(v, int):         # cast only pure ints
            out[k] = float(v)
        else:
            out[k] = v
    return out


# ------------------------- Flattener -------------------------

class Flattener:
    """
    Implements the flattening logic with configurable options.
    """

    def __init__(
        self,
        joiner: str = "_",
        include_parent_fields: str = "all",
        numeric_to_float: bool = False,
    ):
        """
        include_parent_fields:
            - "all": inherit all scalar fields from ancestor dicts (default)
            - "none": do not carry any ancestor fields (only the array slice fields)
            - "selected": provide a whitelist via `set_selected_parent_prefixes()`
        numeric_to_float:
            - If True, cast ints to floats in final rows (to match sample output style).
        """
        allowed = {"all", "none", "selected"}
        if include_parent_fields not in allowed:
            raise ValueError(f"include_parent_fields must be one of {allowed}")
        self.joiner = joiner
        self.include_parent_fields = include_parent_fields
        self.numeric_to_float = numeric_to_float
        self.selected_parent_prefixes: Optional[List[str]] = None

    def set_selected_parent_prefixes(self, prefixes: List[str]) -> None:
        """
        Only used if include_parent_fields == "selected".
        Example: ["auditData", "entId", "product_code", "product_Id_id"]
        """
        self.selected_parent_prefixes = prefixes

    # ------------------------- Public API -------------------------

    def flatten_to_rows(self, data: JSONType) -> List[Dict[str, Any]]:
        """
        Entrypoint. Returns a list of row dicts.
        """
        rows: List[Dict[str, Any]] = []

        # Build an initial "inherited" map of scalar fields found by walking dict-only paths.
        inherited: Dict[str, Any] = {}
        self._collect_inherited_scalars(data, path="", inherited=inherited)

        # Now traverse again to expand arrays into rows, carrying inherited scalars.
        self._expand_arrays(
            data,
            path="",
            inherited=self._filter_inherited(inherited),
            out_rows=rows,
        )

        # Optional numeric cast
        if self.numeric_to_float:
            rows = [coerce_numbers_to_float(r) for r in rows]

        return rows

    # ------------------------- Core logic -------------------------

    def _collect_inherited_scalars(
        self, node: JSONType, path: str, inherited: Dict[str, Any]
    ) -> None:
        """
        Walk down dict-only branches accumulating scalar fields (prefixed),
        but do not emit rows here. Arrays are not expanded in this pass.
        """
        if isinstance(node, dict):
            scalars, nested = split_dict(node)
            if scalars:
                inherited.update(prefix_keys(scalars, path, self.joiner))
            for k, v in nested.items():
                if isinstance(v, dict):
                    child_path = f"{path}{self.joiner}{k}" if path else k
                    self._collect_inherited_scalars(v, child_path, inherited)
        # lists/scalars ignored here

    def _expand_arrays(
        self,
        node: JSONType,
        path: str,
        inherited: Dict[str, Any],
        out_rows: List[Dict[str, Any]],
    ) -> None:
        """
        Expand arrays into rows. For dicts, recurse; for arrays, emit rows.
        """
        if isinstance(node, dict):
            _, nested = split_dict(node)
            for k, v in nested.items():
                child_path = f"{path}{self.joiner}{k}" if path else k
                if isinstance(v, list):
                    self._handle_list(v, child_path, inherited, out_rows)
                elif isinstance(v, dict):
                    # Dict under dict: continue recursion (arrays within will be handled)
                    self._expand_arrays(v, child_path, inherited, out_rows)

        elif isinstance(node, list):
            # If the root is a list, expand it
            self._handle_list(node, path, inherited, out_rows)

        # scalars at root produce no rows by themselves

    def _handle_list(
        self,
        arr: List[Any],
        path: str,
        inherited: Dict[str, Any],
        out_rows: List[Dict[str, Any]],
    ) -> None:
        """
        Expand a list into rows:
          - list of scalars: 1 row per value under its column (path)
          - list of dicts: emit a "parent row" if the element has scalar fields,
            then expand any nested arrays under that element (with those scalars inherited)
          - mixed lists: emit rows for scalar items and also handle dict items
        """
        if not arr:
            return

        any_dict = any(isinstance(x, dict) for x in arr)
        any_scalar_item = any(is_scalar(x) for x in arr)

        # Emit rows for scalar elements (even in mixed lists)
        if any_scalar_item:
            for val in arr:
                if is_scalar(val):
                    row = self._new_row(inherited)
                    row[path] = val
                    row["tableName"] = path
                    out_rows.append(row)

        # Handle dict entries (arrays of dicts or mixed)
        if any_dict:
            for el in arr:
                if not isinstance(el, dict):
                    continue

                scalars, nested = split_dict(el)
                prefixed_scalars = prefix_keys(scalars, path, self.joiner)

                # Emit a parent row if this element has its own scalar fields
                if prefixed_scalars:
                    row = self._new_row(inherited)
                    row.update(prefixed_scalars)
                    row["tableName"] = path
                    out_rows.append(row)

                # Recurse into nested dicts/lists under this element
                # Inherit parent + this element's scalars
                next_inherited = self._new_row(inherited)
                next_inherited.update(prefixed_scalars)
                for k, v in nested.items():
                    child_path = f"{path}{self.joiner}{k}"
                    if isinstance(v, list):
                        self._handle_list(v, child_path, next_inherited, out_rows)
                    elif isinstance(v, dict):
                        # Collect this dict's scalars into the inheritance before deeper recursion
                        scalars_v, _nested_v = split_dict(v)
                        deeper_inherited = self._new_row(next_inherited)
                        if scalars_v:
                            deeper_inherited.update(prefix_keys(scalars_v, child_path, self.joiner))
                        self._expand_arrays(v, child_path, deeper_inherited, out_rows)

    # ------------------------- Helpers -------------------------

    def _filter_inherited(self, inherited: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reduce inherited fields based on include_parent_fields mode.
        """
        if self.include_parent_fields == "all":
            return dict(inherited)
        if self.include_parent_fields == "none":
            return {}
        # selected
        if not self.selected_parent_prefixes:
            return {}
        out: Dict[str, Any] = {}
        for k, v in inherited.items():
            if any(k.startswith(p) for p in self.selected_parent_prefixes):
                out[k] = v
        return out

    @staticmethod
    def _new_row(base: Dict[str, Any]) -> Dict[str, Any]:
        return copy.deepcopy(base)


# ------------------------- CLI -------------------------

def main():
    parser = argparse.ArgumentParser(description="Flatten nested JSON (arrays -> rows).")
    parser.add_argument("--in", dest="infile", type=str, help="Input JSON file path.")
    parser.add_argument("--out", dest="outfile", type=str, help="Output JSON file path.")
    parser.add_argument(
        "--joiner", type=str, default="_", help="Path joiner for column names (default: _)."
    )
    parser.add_argument(
        "--inherit",
        type=str,
        default="all",
        choices=["all", "none", "selected"],
        help="How to inherit parent fields into rows.",
    )
    parser.add_argument(
        "--selected-prefix",
        action="append",
        default=None,
        help="Used if --inherit selected. Repeat: --selected-prefix auditData --selected-prefix entId ...",
    )
    parser.add_argument(
        "--numeric-to-float",
        action="store_true",
        help="Cast ints to floats in final rows (to match sample schema style).",
    )
    args = parser.parse_args()

    # Load input JSON (file or stdin)
    if args.infile:
        with open(args.infile, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        import sys
        data = json.load(sys.stdin)

    flattener = Flattener(
        joiner=args.joiner,
        include_parent_fields=args.inherit,
        numeric_to_float=args.numeric_to_float,
    )
    if args.inherit == "selected" and args.selected_prefix:
        flattener.set_selected_parent_prefixes(args.selected_prefix)

    rows = flattener.flatten_to_rows(data)

    # Write output
    if args.outfile:
        with open(args.outfile, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, ensure_ascii=False)
    else:
        print(json.dumps(rows, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
