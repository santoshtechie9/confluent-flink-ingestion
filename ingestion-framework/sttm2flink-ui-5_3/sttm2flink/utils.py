from __future__ import annotations
import io, zipfile, json, difflib
from typing import List
from .models import GeneratorOptions

# --- Utility helpers preserved for backward compatibility ---
def remove_sql_comments(text: str) -> str:
    """Strip single-line '--' comments."""
    lines = []
    for line in text.splitlines():
        if "--" in line:
            line = line.split("--", 1)[0]
        lines.append(line)
    return "\n".join(lines)

def normalize_sql_whitespace(text: str) -> str:
    """Collapse excessive whitespace and blank lines without altering order."""
    norm = "\n".join([" ".join(line.split()) for line in text.splitlines()])
    norm = "\n".join([l for l in norm.splitlines() if l.strip()])
    return norm.strip()

def compute_diff(a: str, b: str) -> str:
    """Unified diff between two SQL strings."""
    diff_lines = list(difflib.unified_diff(a.splitlines(), b.splitlines(), lineterm=""))
    return "\n".join(diff_lines) or "No differences."

# --- Bundling: exactly three grouped files ---
def bundle_outputs_zip(items, options: GeneratorOptions, validation: dict | None = None) -> bytes:
    """Produce exactly three grouped files (omit empties):

      - bundle/create.sql               all CREATE TABLE statements
      - bundle/views.sql                all CREATE VIEW statements
      - bundle/inserts_statement_set.sql   EXECUTE STATEMENT SET wrapping ALL INSERTs

    Each statement is terminated with ';' and separated by one blank line.
    Validation assets remain under validation/ when enabled.
    """
    creates: List[str] = []
    views: List[str] = []
    inserts: List[str] = []

    for item in items:
        sql = (item.sql or "").strip()
        if not sql:
            continue
        if not sql.endswith(";"):
            sql += ";"
        op = (item.op or "").upper()
        if op == "CREATE":
            creates.append(sql)
        elif op == "VIEW":
            views.append(sql)
        elif op == "INSERT":
            inserts.append(sql)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Grouped outputs (omit empty groups)
        if creates:
            zf.writestr("bundle/create.sql", "\n\n".join(creates) + "\n")
        if views:
            zf.writestr("bundle/views.sql", "\n\n".join(views) + "\n")
        if inserts:
            body = "\n  ".join(inserts)
            stmtset = f"EXECUTE STATEMENT SET\nBEGIN\n  {body}\nEND;\n"
            zf.writestr("bundle/inserts_statement_set.sql", stmtset)

        # Validation assets
        if options.emit_validation_report and validation is not None:
            zf.writestr("validation/summary.json", json.dumps(validation, indent=2))

        if options.emit_validation_report:
            try:
                from .validation import validate_sql_with_sqlglot
                rep = validate_sql_with_sqlglot(items, read_dialect="hive")
                import csv
                out = io.StringIO()
                writer = csv.DictWriter(out, fieldnames=["Result","SQL"])
                writer.writeheader()
                for r in rep:
                    writer.writerow({"Result": r.get("Result",""), "SQL": r.get("SQL","")})
                zf.writestr("validation/sqlglot_report.csv", out.getvalue())
            except Exception as e:
                zf.writestr("validation/sqlglot_report_error.txt", str(e))

    buf.seek(0)
    return buf.read()