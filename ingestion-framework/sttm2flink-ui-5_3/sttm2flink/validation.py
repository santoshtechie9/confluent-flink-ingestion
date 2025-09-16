from __future__ import annotations
from typing import List, Dict
import sqlglot

def validate_sql_with_sqlglot(items, read_dialect: str = "hive") -> List[Dict[str,str]]:
    report: List[Dict[str,str]] = []
    for it in items:
        sqltxt = it.sql.strip()
        result = "OK"
        rendered = sqltxt
        try:
            node = sqlglot.parse_one(sqltxt, read=read_dialect)
            try:
                rendered = node.sql(dialect=read_dialect)
            except Exception:
                rendered = sqltxt
        except Exception:
            result = "ERROR"
            rendered = sqltxt
        report.append({"Result": result, "SQL": rendered})
    return report