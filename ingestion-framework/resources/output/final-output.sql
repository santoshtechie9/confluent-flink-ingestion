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

create view hm_cba_ci_xref_vw (cba_ci_xref_userid, cba_ci_xref_f2, cba_ci_xref_f3) as
select
  json_value(cast(val as string), '$.cba_ci_xref_userid'),
  json_value(cast(val as string), '$.cba_ci_xref_f2'),
  json_value(cast(val as string), '$.cba_ci_xref_f3')
from hm_db
where
  json_value(cast(val as string), '$.tbl') = 'cba_ci_xref';

create view hm_cba_ci_adr_vw (cba_ci_adr_userid, cba_ci_adr_line, cba_ci_adr_city, cba_ci_adr_state) as
select
  json_value(cast(val as string), '$.cba_ci_adr_userid'),
  json_value(cast(val as string), '$.cba_ci_adr_line'),
  json_value(cast(val as string), '$.cba_ci_adr_city'),
  json_value(cast(val as string), '$.cba_ci_adr_state')
from hm_db
where
  json_value(cast(val as string), '$.tbl') = 'cba_ci_adr';


-- ===== SINK TABLES =====

drop table if exists hm_cba_ci;
create table hm_cba_ci (
  cba_ci_userid string,
  cba_ci_name string,
  cba_ci_createdon date,
  cba_ci_modifiedon timestamp_ltz(3)
)
with ('value.format'='avro-registry');

drop table if exists hm_cba_ci_xref;
create table hm_cba_ci_xref (
  cba_ci_xref_userid string,
  cba_ci_xref_f2 string,
  cba_ci_xref_f3 string
)
with ('value.format'='avro-registry');

drop table if exists hm_cba_ci_adr;
create table hm_cba_ci_adr (
  cba_ci_adr_userid string,
  cba_ci_adr_line string,
  cba_ci_adr_city string,
  cba_ci_adr_state string
)
with ('value.format'='avro-registry');

drop table if exists hm_cba_ci_adr_fgac;
create table hm_cba_ci_adr_fgac (
  cba_ci_adr_userid string,
  cba_ci_adr_line string,
  cba_ci_adr_city string,
  cba_ci_adr_state string,
  cba_ci_xref_f2 string,
  cba_ci_xref_f3 string
)
with ('value.format'='avro-registry');

drop table if exists hm_cba_ci_quarantine;
create table hm_cba_ci_quarantine (
  cba_ci_user_id string,
  what string
)
with ('value.format'='avro-registry');


-- ===== INSERTS =====
execute statement set
begin
INSERT INTO hm_cba_ci (cba_ci_userid, cba_ci_name, cba_ci_createdon, cba_ci_modifiedon)
SELECT
  cba_ci_userid AS cba_ci_userid,
  cba_ci_name AS cba_ci_name,
  case
  when char_length(cba_ci_createdon) = 8 and regexp(cba_ci_createdon, '^\d{8}$')
    then to_date(cba_ci_createdon, 'yyyyMMdd')
  when char_length(cba_ci_createdon) = 10 and regexp(cba_ci_createdon, '^\d{4}-\d{2}-\d{2}$')
    then to_date(cba_ci_createdon, 'yyyy-MM-dd')
  else null
end AS cba_ci_createdon,
  case
  when char_length(cba_ci_modifiedon) = 23 and regexp(cba_ci_modifiedon, '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$')
    then to_timestamp_ltz(cba_ci_modifiedon, 'yyyy-MM-dd hh:mm:ss.SSS')
  when char_length(cba_ci_modifiedon) = 10 and is_decimal(cba_ci_modifiedon)
     to_timestamp_ltz(COALESCE(TRY_CAST(cba_ci_modifiedon AS bigint)*1000, 0), 3)
  when char_length(cba_ci_modifiedon) = 13 and is_decimal(cba_ci_modifiedon)
    then to_timestamp_ltz(COALESCE(TRY_CAST(cba_ci_modifiedon AS bigint), 0), 3)
  else null
end AS cba_ci_modifiedon
FROM hm_cba_ci_vw;

INSERT INTO hm_cba_ci_xref (cba_ci_xref_userid, cba_ci_xref_f2, cba_ci_xref_f3)
SELECT
  cba_ci_xref_userid AS cba_ci_xref_userid,
  cba_ci_xref_f2 AS cba_ci_xref_f2,
  cba_ci_xref_f3 AS cba_ci_xref_f3
FROM hm_cba_ci_xref_vw;

INSERT INTO hm_cba_ci_adr (cba_ci_adr_userid, cba_ci_adr_line, cba_ci_adr_city, cba_ci_adr_state)
SELECT
  cba_ci_adr_userid AS cba_ci_adr_userid,
  cba_ci_adr_line AS cba_ci_adr_line,
  cba_ci_adr_city AS cba_ci_adr_city,
  cba_ci_adr_state AS cba_ci_adr_state
FROM hm_cba_ci_adr_vw;

INSERT INTO hm_cba_ci_adr_fgac (cba_ci_adr_userid, cba_ci_adr_line, cba_ci_adr_city, cba_ci_adr_state, cba_ci_xref_f2, cba_ci_xref_f3)
SELECT
  hm_cba_ci_adr_vw.cba_ci_adr_userid AS cba_ci_adr_userid,
  hm_cba_ci_adr_vw.cba_ci_adr_line AS cba_ci_adr_line,
  hm_cba_ci_adr_vw.cba_ci_adr_city AS cba_ci_adr_city,
  hm_cba_ci_adr_vw.cba_ci_adr_state AS cba_ci_adr_state,
  hm_cba_ci_xref_vw.cba_ci_xref_f2 AS cba_ci_xref_f2,
  hm_cba_ci_xref_vw.cba_ci_xref_f3 AS cba_ci_xref_f3
FROM hm_cba_ci_adr_vw
LEFT OUTER JOIN hm_cba_ci_xref_vw ON hm_cba_ci_adr_vw.cba_ci_adr_userid = hm_cba_ci_xref_vw.cba_ci_xref_userid;

INSERT INTO hm_cba_ci_quarantine (cba_ci_user_id, what)
SELECT
  hm_cba_ci_adr_vw.cba_ci_adr_userid AS cba_ci_user_id,
  'xref: none' AS what
FROM hm_cba_ci_adr_vw
LEFT OUTER JOIN hm_cba_ci_xref_vw ON hm_cba_ci_adr_vw.cba_ci_adr_userid = hm_cba_ci_xref_vw.cba_ci_xref_userid
WHERE hm_cba_ci_xref_vw.cba_ci_xref_userid IS NULL
UNION ALL
SELECT
  hm_cba_ci_xref_vw.cba_ci_xref_userid AS cba_ci_user_id,
  'adr: none' AS what
FROM hm_cba_ci_xref_vw
LEFT OUTER JOIN hm_cba_ci_adr_vw ON hm_cba_ci_xref_vw.cba_ci_xref_userid = hm_cba_ci_adr_vw.cba_ci_adr_userid
WHERE hm_cba_ci_adr_vw.cba_ci_adr_userid IS NULL;
end;
