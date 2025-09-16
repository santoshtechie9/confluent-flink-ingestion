from __future__ import annotations
from pydantic import BaseModel
from typing import Optional, Literal

class GeneratorOptions(BaseModel):
    # formatting
    format_sql: bool = True
    normalize_whitespace: bool = True
    remove_comments: bool = True
    dialect: str = "flinksql"
    # outputs
    emit_validation_report: bool = True
    emit_create: bool = True
    emit_dml: bool = True
    emit_view: bool = False
    emit_statement_set: bool = False
    # input format
    sttm_format: Literal["auto","column-spec","row-per-table"] = "auto"
    excel_sheet: Optional[str] = None
    # naming
    name_prefix: str = ""
    view_suffix: str = ""  # blank by default to match v5 behavior
    # config application
    apply_config_to: Literal["none","tables","views"] = "none"