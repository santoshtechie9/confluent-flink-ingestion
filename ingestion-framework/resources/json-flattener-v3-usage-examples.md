Got it 👍 — here’s a **single documentation file** (`USAGE_EXAMPLES_V2.1.md`) that collects **all usage examples for the patched v2.1 flattener**.
You can keep this file with the script so your team has a ready guide.

---

````markdown
# JSON Flattener v2.1 – Usage Examples

This guide shows how to use `json_flattener_v2.1.py` in different modes.  
The script flattens nested JSON into table-like rows, adding lineage, safety guards, and schema manifests.

---

## 1. Flatten a JSON file into a single JSON array

```bash
python json_flattener_v2.1.py --in event.json --out rows.json
````

* Input: `event.json` (nested JSON).
* Output: `rows.json` containing an array of flattened row dicts.
* Good for inspection or export to Pandas/Excel.

---

## 2. Stream as NDJSON (newline-delimited JSON)

```bash
python json_flattener_v2.1.py --in event.json --ndjson > rows.ndjson
```

* Each row is emitted as its own JSON object per line.
* Best for streaming pipelines (Kafka, Flink, Splunk).

**Example lines:**

```json
{"tableName": "product_closingOnly", "_row_id": 1, "_parent_id": null, "_path": "/product/closingOnly/0", "_elem_index": 0, "_depth": 2, "product_closingOnly": 1}
{"tableName": "product_closingOnly", "_row_id": 2, "_parent_id": null, "_path": "/product/closingOnly/1", "_elem_index": 1, "_depth": 2, "product_closingOnly": 2}
```

---

## 3. Emit parent stub rows for empty elements

```bash
python json_flattener_v2.1.py --in deep.json --out rows.json --emit-empty-parent
```

* If an array element has no scalars but has nested arrays, still emits a **stub parent row** to preserve lineage.

---

## 4. Apply safety guards

```bash
python json_flattener_v2.1.py --in big.json --ndjson \
  --max-depth 8 \
  --max-rows 5000 \
  --max-cols 40
```

* Stops traversal at depth > 8.
* Emits at most 5,000 rows.
* Trims each row to at most 40 columns.
* Prints warnings on stderr when trimming happens:

  ```
  [WARN] Row trimmed from 75 to 40 cols
  ```

---

## 5. Generate schema manifest

```bash
python json_flattener_v2.1.py --in event.json --ndjson --schema-out manifest.json
```

* Creates `manifest.json` with observed columns and types per `tableName`.

**Example snippet:**

```json
{
  "product_closingOnly": {
    "columns": [
      "_depth",
      "_elem_index",
      "_parent_id",
      "_path",
      "_row_id",
      "product_closingOnly",
      "tableName"
    ],
    "types": {
      "_depth": ["int"],
      "_elem_index": ["int"],
      "_parent_id": ["NoneType","int"],
      "_path": ["str"],
      "_row_id": ["int"],
      "product_closingOnly": ["int"],
      "tableName": ["str"]
    }
  }
}
```

---

## 6. Preserve numeric precision but keep booleans

```bash
python json_flattener_v2.1.py --in event.json --ndjson --numeric-to-float
```

* Integers are cast to floats (`123` → `123.0`).
* Booleans stay `true/false` (not `1.0/0.0`).

---

## 7. Use stdin/stdout in pipelines

```bash
cat event.json | python json_flattener_v2.1.py --ndjson | head -n 5
```

* Reads JSON from stdin.
* Streams rows to stdout.
* Works with pipes into Spark, Flink, or Unix tools.

---

## 8. Observe lineage fields

Every row carries lineage metadata:

* `_row_id`: unique identifier per row
* `_parent_id`: row id of parent (if any)
* `_path`: full JSON pointer (e.g., `/product/tsf/0/tradeSourceId/1`)
* `_elem_index`: index of this element in its parent array
* `_depth`: nesting depth

**Example row:**

```json
{
  "tableName": "product_tsf_tradeSourceId",
  "_row_id": 15,
  "_parent_id": 14,
  "_path": "/product/tsf/0/tradeSourceId/1",
  "_elem_index": 1,
  "_depth": 4,
  "product_tsf_businessDateAdded": "2018-02-01",
  "product_tsf_tradeSourceId": 30
}
```

---

## Tips

* Use **`--ndjson`** for scalability (row-by-row streaming).
* Use **`--emit-empty-parent`** if you want every array element represented, even empty ones.
* Always generate a schema manifest (`--schema-out`) if downstream consumers expect a stable schema.
* Guards (`--max-depth`, `--max-rows`, `--max-cols`) protect against runaway flattening.

---

```

---

Would you like me to also **bundle a demo `event.json` input file** (small but nested with arrays + scalars) so that every example here can be executed directly without extra prep?
```
