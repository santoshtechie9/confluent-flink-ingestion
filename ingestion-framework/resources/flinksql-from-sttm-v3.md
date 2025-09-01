
````markdown
# Flink SQL Generator (v3)

This tool reads a **Source-to-Target Mapping (STTM) Excel workbook** and produces a **single combined Flink SQL file** containing:

1. `CREATE VIEW` statements (one per source table, extracting JSON fields).
2. `CREATE TABLE` sink definitions (one per target table, with `WITH (...)` connector options).
3. `INSERT` statements (one per target table; if multiple union arms exist, they’re combined with `UNION ALL`).

All SQL is generated dynamically from the STTM file — no hardcoding.

---

## Input

The workbook must contain:

- **Config** sheet  
  | Key                       | Value             |
  |---------------------------|-------------------|
  | raw_table_name            | hm_db             |
  | raw_value_column          | val               |
  | table_identifier_field    | $.tbl             |
  | view_prefix               | hm_               |
  | view_suffix               | _vw               |
  | sink_value_format         | avro-registry     |
  | wrap_in_statement_set     | true              |
  | target_with.connector     | jdbc              |
  | with.hm_cba_ci.table-name | CBA_CI            |

- **STTM** sheet  
  Columns:  
  `Source Table, Source Column, Data Type, Target Table, Target Column, Target Data Type, Expression, Filter, Join Order, Join Type, Join Condition`

---

## Usage

Run from CLI:

```bash
python flinksql_from_sttm.py \
  --excel STTM_from_images_full.xlsx \
  --out-file flink_sql_combined.sql
````

---

## Example Output (excerpt)

```sql
-- ===== VIEWS =====
create view hm_cba_ci_vw (cba_ci_userid, cba_ci_name, cba_ci_createdon, cba_ci_modifiedon) as
select
  json_value(cast(val as string), '$.cba_ci_userid'),
  json_value(cast(val as string), '$.cba_ci_name'),
  json_value(cast(val as string), '$.cba_ci_createdon'),
  json_value(cast(val as string), '$.cba_ci_modifiedon')
from hm_db
where
  json_value(cast(val as string), '$.tbl') = 'cba_ci';

-- ===== SINK TABLES =====
create table hm_cba_ci (
  cba_ci_userid string,
  cba_ci_name string,
  cba_ci_createdon date,
  cba_ci_modifiedon timestamp_ltz(3)
) with ('value.format'='avro-registry');

-- ===== INSERTS =====
execute statement set
begin
insert into hm_cba_ci (cba_ci_userid, cba_ci_name, cba_ci_createdon, cba_ci_modifiedon)
select
  cba_ci_userid,
  cba_ci_name,
  case
    when char_length(cba_ci_createdon) = 8 and regexp(cba_ci_createdon, '^\d{8}$')
      then to_date(cba_ci_createdon, 'yyyyMMdd')
    when char_length(cba_ci_createdon) = 10 and regexp(cba_ci_createdon, '^\d{4}-\d{2}-\d{2}$')
      then to_date(cba_ci_createdon, 'yyyy-MM-dd')
    else null
  end as cba_ci_createdon,
  ...
from hm_cba_ci_vw;
end;
```

---

## Reuse

* To adapt for another schema, only the **STTM workbook** needs to be updated (new source/target tables, columns, joins, expressions).
* The generator script remains the same.

---

```

Do you want me to also generate a **ready-to-paste README.md** file that includes this usage block plus a short “Overview” and “Installation” section?
```
