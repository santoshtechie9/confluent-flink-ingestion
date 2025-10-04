Here’s a clear, no-fluff reference for the **single-sheet STTM** we’ve finalized (Option A) with **MASTER** views and **materialized XREF** snapshot tables.

# STTM layout (single sheet)

## Core columns (existing)

1. **source_table**

   * For `MASTER`: the logical table name embedded in the master Kafka payload (e.g., `cust`, `order`).
   * For `XREF`: the **xref table name** you want created (e.g., `cust_xref`).

2. **source_column**

   * For `MASTER`: JSON field to project into the view.
   * For `XREF`: column to include in the xref table (must exist in the referenced MASTER view).

3. **target_table**

   * Used for **final sinks** (not for MASTER/XREF). When set, this row contributes to a target table DDL + its INSERT mapping.

4. **target_column**

   * Column name in the **target_table**. If blank, falls back to `source_column`.

5. **target_data_type**

   * Data type for `target_column` in **target_table** DDL. If blank, generator falls back to defaults (e.g., `STRING`) unless configured otherwise.

6. **expression**

   * SQL expression used in **INSERT** select lists for **target_table** mappings (e.g., casts, concatenations).
   * If blank, generator uses the qualified `source_column` (or the alias created in the view).

7. **filter**

   * Optional `WHERE` predicate applied to the SELECT arm that generates the INSERT. Rows sharing the same `(target_table, filter, join_condition)` get **UNION ALL**’d.

8. **join_order** (integer)

   * Determines the **driving view** in a multi-view join for a `target_table`. Lowest number = driving side.

9. **join_type**

   * Join type for the join to the **next** view (`LEFT/RIGHT/FULL` → normalized to `LEFT OUTER/RIGHT OUTER/FULL OUTER`).

10. **join_condition**

* SQL `ON ...` predicate between views (e.g., `a_vw.cust_id = b_vw.cust_id`).

> These 10 columns keep the original pipeline behavior for **final target sinks & INSERTs**.

---

## New columns (to support MASTER + XREF)

11. **source_kind** — `MASTER` | `XREF`

* `MASTER` rows define which JSON fields to project into the **master-derived view** for that `source_table`.
* `XREF` rows define a **materialized snapshot table** derived from a MASTER view.

12. **xref_from** (required for `XREF`)

* The MASTER **source_table** to derive from (generator reads from `<view_prefix><xref_from><view_suffix>`).

13. **source_pk** (required for `XREF`)

* Comma-separated primary key columns (supports composite PK). Used for xref table DDL and `PARTITION BY` in dedupe window.

14. **event_ts_field** (required for `XREF`; optional for `MASTER`)

* Timestamp column present in the MASTER view used to pick the **latest** record per key.

15. **seq_field** (optional for `XREF`)

* Numeric/monotonic sequence used as a **tie-breaker** when `event_ts_field` ties (e.g., `op_seq`).

16. **delete_flag_field** (optional for `XREF`)

* Soft-delete flag column name present in the MASTER view (e.g., `is_deleted`).

17. **delete_flag_values** (optional for `XREF`)

* CSV list of “truthy” values to **exclude** (e.g., `true,1`). Applied in the `WHERE` clause before ranking.

---

# Kinds of tables the generator creates

## 1) MASTER-derived views (from a master Kafka topic)

* Identified by distinct `source_table` values where `source_kind=MASTER`.
* Columns = all distinct `source_column` values for that `(source_kind=MASTER, source_table)` group (plus `event_ts_field`/`seq_field` if listed).
* View name: `<view_prefix><source_table><view_suffix>` (from Config; defaults like `hm_` + table + `_vw`).
* SQL pattern (simplified):

  ```sql
  CREATE VIEW hm_<source_table>_vw (<cols...>) AS
  SELECT
    json_value(CAST(<raw_value_column> AS STRING), '$.<col1>') AS <col1>,
    ...
  FROM <raw_table_name>
  WHERE json_value(CAST(<raw_value_column> AS STRING), '<table_identifier_field>') = '<source_table>';
  ```
* Purpose: provide clean, per-table projections filtered from a **single master topic** carrying multiple entities.

## 2) XREF snapshot **tables** (materialized latest-by-key)

* Declared by rows where `source_kind=XREF`.
* Each **XREF group** is keyed by `source_table` (the xref’s name).
* Must specify: `xref_from`, `source_pk`, `event_ts_field`.
* Generator emits:

  * **CREATE TABLE** with `PRIMARY KEY (<source_pk...>) NOT ENFORCED`.
  * **INSERT** that selects **latest per key** from the referenced MASTER view, optionally filtering deletes, ordering by `event_ts_field` and `seq_field` (if provided).
* SQL pattern (simplified):

  ```sql
  CREATE TABLE <xref_table> (
    <declared columns...>,
    PRIMARY KEY (<pk...>) NOT ENFORCED
  ) WITH (...upsert/iceberg props...);

  INSERT INTO <xref_table>
  SELECT * FROM (
    SELECT
      <columns...>,
      ROW_NUMBER() OVER (
        PARTITION BY <pk...>
        ORDER BY <event_ts_field> DESC, <seq_field_if_present> DESC
      ) AS rn
    FROM <view_prefix><xref_from><view_suffix>
    WHERE (<delete predicate if configured>)
  ) WHERE rn = 1;
  ```

## 3) Final **target sinks** (unchanged)

* When `target_table` is set, rows contribute to:

  * `DROP TABLE IF EXISTS` + `CREATE TABLE` (column types from `target_data_type`), and
  * `INSERT INTO <target_table>` built from one or more views, with joins & filters per below.

---

# Expressions, joins, and filters (how inserts are built)

* **expression**

  * Applies to **target** INSERT generation. If provided, it becomes the select expression for the `target_column`.
  * If blank, generator uses the qualified column from the appropriate view (or raw column if single source).

* **join_order**

  * Controls the **driving** view (smallest `join_order`). Other views join onto it in ascending order.

* **join_type**

  * Accepts `LEFT`, `RIGHT`, `FULL` (normalized to `LEFT/RIGHT/FULL OUTER`). Inner joins are the default when not specified (same as left with restrictive condition?—we preserve existing generator behavior).

* **join_condition**

  * SQL `ON` clause between the current and next view (e.g., `a_vw.cust_id = b_vw.cust_id`).
  * The generator auto-qualifies column references by view aliasing.

* **filter**

  * Optional `WHERE` for that “arm”. Rows with the same `(target_table, filter, join_condition|NOJOIN)` are grouped and combined with **UNION ALL**.

* **Column qualification**

  * Single-view selects: bare column names.
  * Multi-view joins: fully qualified `<view>.<column>` to avoid ambiguity.

---

# Validation rules the generator enforces

* For each **MASTER**:

  * Must have ≥1 `source_column`.
* For each **XREF**:

  * `xref_from` must exist and be a **MASTER** `source_table`.
  * `source_pk` and `event_ts_field` are **required** and must exist in the `xref_from` MASTER view’s column set.
  * `seq_field`/`delete_flag_field` (if given) must exist in that MASTER view.
  * Every XREF `source_column` must exist in the MASTER view; otherwise a clear error is emitted.
* **Targets**:

  * `target_table` DDL uses `target_data_type`; if missing, safe defaults are applied.
  * Joins without `join_condition` in a multi-view scenario produce an error.

---

# Where options live (Config sheet, unchanged unless you add overrides)

* **Global**

  * `raw_table_name`, `raw_value_column`, `table_identifier_field` (e.g., `$.tbl`), `view_prefix`, `view_suffix`, `sink value.format`, `wrap_in_statement_set`.

* **WITH properties**

  * Global target props: `target_with.*`
  * Per-target overrides: `with.<target_table>.*`
  * (Optional) If you want: `xref.sink.default.*` and `xref.sink.<xref_table>.*` for xref-specific sink options; otherwise xrefs inherit target defaults.

* **Outputs**

  * `views.yaml` (MASTER views), `xref.yaml` (xref DDL+INSERT), `sinks.yaml` (target DDLs), `inserts.yaml` (target INSERTs), `out.sql_validation.csv` (parse results).

---

## Tiny, realistic example (5 rows per block)

**MASTER (cust)**

```
source_kind | source_table | source_column | ... | event_ts_field | seq_field
MASTER      | cust         | cust_id       | ... | event_ts       | op_seq
MASTER      | cust         | cust_name     | ... | event_ts       | op_seq
MASTER      | cust         | status        | ... | event_ts       | op_seq
MASTER      | cust         | event_ts      | ... | event_ts       | op_seq
MASTER      | cust         | op_seq        | ... | event_ts       | op_seq
```

**XREF (cust_xref from cust)**

```
source_kind | source_table | source_column | xref_from | source_pk | event_ts_field | seq_field | delete_flag_field | delete_flag_values
XREF        | cust_xref    | cust_id       | cust      | cust_id   | event_ts       | op_seq    | is_deleted        | true,1
XREF        | cust_xref    | cust_name     | cust      | cust_id   | event_ts       | op_seq    | is_deleted        | true,1
XREF        | cust_xref    | status        | cust      | cust_id   | event_ts       | op_seq    | is_deleted        | true,1
XREF        | cust_xref    | event_ts      | cust      | cust_id   | event_ts       | op_seq    | is_deleted        | true,1
XREF        | cust_xref    | op_seq        | cust      | cust_id   | event_ts       | op_seq    | is_deleted        | true,1
```

**TARGET (orders_enriched) – joined example**

```
source_kind | source_table | source_column | target_table     | target_column   | target_data_type | expression                   | join_order | join_type | join_condition
MASTER      | order        | order_id      | orders_enriched  | order_id        | STRING           |                             | 1          |           |
MASTER      | order        | cust_id       | orders_enriched  | cust_id         | STRING           |                             | 1          |           |
XREF        | cust_xref    | status        | orders_enriched  | customer_status | STRING           |                             | 2          | LEFT      | order_vw.cust_id = cust_xref.cust_id
```

---

# A “top 1%” prompt you can reuse with me

Copy-paste this and fill the bracketed parts. It is surgical, requests clarifications where needed, and prevents hallucination.

> **Role:** Act as an expert Python engineer for my STTM→Flink SQL generator.
> **Inputs I will provide now:**
>
> 1. The Excel workbook (single-sheet STTM) with columns:
>    `source_kind, source_table, source_column, target_table, target_column, target_data_type, expression, filter, join_order, join_type, join_condition, xref_from, source_pk, event_ts_field, seq_field, delete_flag_field, delete_flag_values`.
> 2. The Config sheet with keys like `raw_table_name, raw_value_column, table_identifier_field, view_prefix, view_suffix, wrap_in_statement_set, target_with.*`, and optional `with.<table>.*` / `xref.sink.*`.
> 3. The current Python generator script.
>
> **What you must do:**
>
> * Validate the workbook strictly against the finalized rules (MASTER view spec, XREF snapshot spec, target mappings, joins).
> * Generate: MASTER view DDLs, XREF table DDLs + latest-by-key INSERTs, target sink DDLs, and INSERTs with joins/filters.
> * Emit `views.yaml`, `xref.yaml`, `sinks.yaml`, `inserts.yaml`, and `out.sql_validation.csv`.
> * Do not invent columns, types, or properties not present in STTM/Config.
>
> **Clarify before executing if any of these are ambiguous:**
>
> 1. **Master topic semantics:** confirm `raw_table_name`, `raw_value_column`, and `table_identifier_field` identify MASTER tables (e.g., `$.tbl`).
> 2. **Timestamp normalization:** if `event_ts_field` needs casting (epoch → `TIMESTAMP_LTZ`) specify exact rule; otherwise treat it as already normalized.
> 3. **XREF sinks:** confirm default sink type and WITH props (inherit target defaults or use `xref.sink.*`).
> 4. **Delete handling:** confirm `delete_flag_field` and `delete_flag_values` meaning; if absent, skip delete filtering.
> 5. **Data type gaps:** if `target_data_type` is missing for any target/xref column, confirm fallback (`STRING`) or provide explicit types.
> 6. **Join aliasing:** confirm expected view aliases (defaults: `<view_prefix><source_table><view_suffix>`).
> 7. **Statement set:** confirm `wrap_in_statement_set` preference.
> 8. **Dialect for validation:** confirm `sqlglot` dialect priority (e.g., `ansi,hive,spark,presto,postgres`).
>
> **Deliverables:**
>
> * The generated SQL/YAML files and the validation CSV.
> * A concise summary of any validation errors or unresolved clarifications (no assumptions).
> * No code changes unless I explicitly request them after review.

If this doc and prompt look good, say the word and I’ll use them as our working contract going forward.
