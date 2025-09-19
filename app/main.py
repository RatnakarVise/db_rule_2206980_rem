from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional, Tuple, Dict, Any
import re
import json
from datetime import datetime

app = FastAPI(
    title="MM-IM Remediator (S4HANA Material Document & Stock Tables)"
)

# -----------------------------
# Reference mappings
# -----------------------------

# Core Document Tables
CORE_DOC_MAP: Dict[str, Dict[str, Any]] = {
    "MKPF": {
        "new": "MATDOC",
        "note": "Header data no longer stored separately. Still exists as DDIC object, but only read via CDS view NSDM_DDL_MKPF."
    },
    "MSEG": {
        "new": "MATDOC",
        "note": "Item + header + attributes merged. Proxy CDS: NSDM_DDL_MSEG."
    },
}

# Hybrid Tables (Master Data + Quantities)
HYBRID_MAP: Dict[str, Dict[str, Any]] = {
    "MARC": {"new": "NSDM_V_MARC", "note": "Plant Data for Material now redirected to CDS views."},
    "MARD": {"new": "NSDM_V_MARD", "note": "Storage location data no longer persisted."},
    "MCHB": {"new": "NSDM_V_MCHB", "note": "Batch stock quantities derived from MATDOC."},
    "MKOL": {"new": "NSDM_V_MKOL", "note": "Special stocks from vendor redirected."},
    "MSLB": {"new": "NSDM_V_MSLB", "note": "Special stocks with vendor derived from MATDOC."},
    "MSKA": {"new": "NSDM_V_MSKA", "note": "Sales order stock redirected."},
    "MSPR": {"new": "NSDM_V_MSPR", "note": "Project stock aggregated on the fly."},
    "MSKU": {"new": "NSDM_V_MSKU", "note": "Special stocks with customer from MATDOC."},
}

# Replaced Aggregation Tables
AGGR_MAP: Dict[str, Dict[str, Any]] = {
    "MSSA": {"new": "NSDM_V_MSSA", "note": "Customer order totals replaced by CDS view."},
    "MSSL": {"new": "NSDM_V_MSSL", "note": "Special stocks with vendor totals replaced by CDS view."},
    "MSSQ": {"new": "NSDM_V_MSSQ", "note": "Project stock totals replaced by CDS view."},
    "MSTB": {"new": "NSDM_V_MSTB", "note": "Stock in transit replaced by CDS view."},
    "MSTE": {"new": "NSDM_V_MSTE", "note": "Stock in transit (SD Doc) replaced by CDS view."},
    "MSTQ": {"new": "NSDM_V_MSTQ", "note": "Stock in transit for project replaced by CDS view."},
}

# DIMP Split Hybrid Tables
DIMP_MAP: Dict[str, Dict[str, Any]] = {
    "MCSD": {"new": "NSDM_V_MCSD", "note": "Customer Stock split: stock → MATDOC, master → MCSD_MD."},
    "MCSS": {"new": "NSDM_V_MCSS", "note": "Customer Stock Total split: stock → MATDOC, master → MCSS_MD."},
    "MSCD": {"new": "NSDM_V_MSCD", "note": "Customer Stock with Vendor split into MATDOC + MSCD_MD."},
    "MSCS": {"new": "NSDM_V_MSCS", "note": "Cust. Stock with Vendor Total split into MATDOC + MSCS_MD."},
    "MSFD": {"new": "NSDM_V_MSFD", "note": "Sales Order Stock with Vendor split into MATDOC + MSFD_MD."},
    "MSFS": {"new": "NSDM_V_MSFS", "note": "Sales Order Stock with Vendor Total split into MATDOC + MSFS_MD."},
    "MSID": {"new": "NSDM_V_MSID", "note": "Vendor Stock split into MATDOC + MSID_MD."},
    "MSIS": {"new": "NSDM_V_MSIS", "note": "Vendor Stock Total split into MATDOC + MSIS_MD."},
    "MSRD": {"new": "NSDM_V_MSRD", "note": "Project Stock with Vendor split into MATDOC + MSRD_MD."},
    "MSRS": {"new": "NSDM_V_MSRS", "note": "Project Stock with Vendor Total split into MATDOC + MSRS_MD."},
}

# History Tables
HISTORY_MAP: Dict[str, Dict[str, Any]] = {
    "MARCH": {"new": "NSDM_V_MARCH", "note": "MARC History redirected to CDS."},
    "MARDH": {"new": "NSDM_V_MARDH", "note": "MARD History redirected to CDS."},
    "MCHBH": {"new": "NSDM_V_MCHBH", "note": "MCHB History redirected to CDS."},
    "MKOLH": {"new": "NSDM_V_MKOLH", "note": "MKOL History redirected to CDS."},
    "MSLBH": {"new": "NSDM_V_MSLBH", "note": "MSLB History redirected to CDS."},
    "MSKAH": {"new": "NSDM_V_MSKAH", "note": "MSKA History redirected to CDS."},
    "MSSAH": {"new": "NSDM_V_MSSAH", "note": "MSSA History redirected to CDS."},
    "MSPRH": {"new": "NSDM_V_MSPRH", "note": "MSPR History redirected to CDS."},
    "MSSQH": {"new": "NSDM_V_MSSQH", "note": "MSSQ History redirected to CDS."},
    "MSKUH": {"new": "NSDM_V_MSKUH", "note": "MSKU History redirected to CDS."},
    "MSTBH": {"new": "NSDM_V_MSTBH", "note": "MSTB History redirected to CDS."},
    "MSTEH": {"new": "NSDM_V_MSTEH", "note": "MSTE History redirected to CDS."},
    "MSTQH": {"new": "NSDM_V_MSTQH", "note": "MSTQ History redirected to CDS."},
    "MCSDH": {"new": "NSDM_V_MCSDH", "note": "MCSD History redirected to CDS."},
    "MCSSH": {"new": "NSDM_V_MCSSH", "note": "MCSS History redirected to CDS."},
    "MSCDH": {"new": "NSDM_V_MSCDH", "note": "MSCD History redirected to CDS."},
    "MSFDH": {"new": "NSDM_V_MSFDH", "note": "MSFD History redirected to CDS."},
    "MSIDH": {"new": "NSDM_V_MSIDH", "note": "MSID History redirected to CDS."},
    "MSRDH": {"new": "NSDM_V_MSRDH", "note": "MSRD History redirected to CDS."},
}

# Merge all tables into one map for detection
TABLE_MAP = {**CORE_DOC_MAP, **HYBRID_MAP, **AGGR_MAP, **DIMP_MAP, **HISTORY_MAP}

# -----------------------------
# Regex
# -----------------------------

TABLE_NAMES = sorted(TABLE_MAP.keys(), key=len, reverse=True)
TABLE_RE = re.compile(
    rf"""
    \b(?P<name>{'|'.join(map(re.escape, TABLE_NAMES))})\b
    """,
    re.IGNORECASE | re.VERBOSE
)

# -----------------------------
# Models
# -----------------------------

class Unit(BaseModel):
    pgm_name: str
    inc_name: str
    type: str
    name: Optional[str] = None
    class_implementation: Optional[str] = None
    # start_line: Optional[int] = None
    # end_line: Optional[int] = None
    original_code: Optional[str] = ""

# -----------------------------
# Helpers
# -----------------------------
# --- SNIPPET HELPER ---
def snippet_at(text: str, start: int, end: int) -> str:
    s = max(0, start - 60)
    e = min(len(text), end + 60)
    return text[s:e]

def _add_hit(
    hits: List[dict],
    span: Tuple[int, int],
    target_name: str,
    suggested_statement: str,
    src: str,
    note: Optional[str] = None
):
    start, end = span
    meta = {
        "table": target_name,
        "target_type": "Table",
        "target_name": target_name,
        # "start_char_in_unit": span[0] if span else None,
        # "end_char_in_unit": span[1] if span else None,
        "used_fields": [],
        "ambiguous": False,
        "suggested_statement": suggested_statement,
        "suggested_fields": None,
        "snippet": snippet_at(src, start, end)
    }
    if note:
        meta["note"] = note
    hits.append(meta)

def add_order_by_to_selects(sql: str) -> str:
    """
    Add ORDER BY clause to SELECT statements if not already present.
    - For SELECT with explicit fields: ORDER BY those fields
    - For SELECT * : ORDER BY primary key placeholders (*)
    """
    def replacer(match: re.Match) -> str:
        full_stmt = match.group(0)
        fields = match.group("fields").strip()

        # If already has ORDER BY, return unchanged
        if "ORDER BY" in full_stmt.upper():
            return full_stmt

        # Handle SELECT *
        if fields == "*":
            order_by = " ORDER BY PRIMARY KEY"  # placeholder, or you can map per table
            return full_stmt.rstrip(".") + order_by + "."

        # Handle named fields
        field_list = fields.split()
        cleaned_fields = [f for f in field_list if f.isidentifier()]

        if not cleaned_fields:
            return full_stmt

        order_by = " ORDER BY " + " ".join(cleaned_fields)
        return full_stmt.rstrip(".") + order_by + "."

    # Regex for SELECT … FROM … (capture field list before FROM)
    select_re = re.compile(
        r"""
        SELECT\s+(?P<fields>\*|.+?)\s+FROM\s+[A-Z0-9_]+.*?(?:\.|\n)
        """,
        re.IGNORECASE | re.VERBOSE | re.DOTALL
    )
    return select_re.sub(replacer, sql)


def remediate_code(txt: str) -> Tuple[str, List[dict]]:
    """
    Replace obsolete MM-IM tables with S/4HANA replacements,
    but skip replacements if table is part of UPDATE/DELETE/MODIFY.
    """
    if not txt:
        return txt, []

    issues: List[dict] = []
    matches = sorted(TABLE_RE.finditer(txt), key=lambda x: x.start())

    last_idx = 0
    remediated_parts = []

    today = datetime.today().strftime("%Y-%m-%d")
    change_marker = f'\n" Changed by PwC on {today}\n'

    for m in matches:
        name = m.group("name").upper()
        info = TABLE_MAP.get(name)
        if not info:
            continue

        line_start = txt.rfind("\n", 0, m.start()) + 1
        line_end = txt.find("\n", m.end())
        if line_end == -1:
            line_end = len(txt)
        line = txt[line_start:line_end].upper()

        # --- Skip UPDATE/DELETE/MODIFY statements ---
        if re.match(r"\s*(UPDATE|MODIFY)\s+" + name + r"\b", line) or re.match(r"\s*DELETE\s+FROM\s+" + name + r"\b", line):
            remediated_parts.append(txt[last_idx:m.end()])
            last_idx = m.end()
            continue

        # Normal replacement
        replacement = info["new"]
        remediated_parts.append(txt[last_idx:m.start()])
        remediated_parts.append(replacement)
        remediated_parts.append(change_marker)  # add PwC comment
        last_idx = m.end()

        # Track issue
        _add_hit(
            issues,
            m.span(),
            name,
            f"Replaced {name} with {replacement}.",
            src=txt,
            note=info.get("note")
        )

    # Add trailing text
    remediated_parts.append(txt[last_idx:])
    new_txt = "".join(remediated_parts)
    # new_txt = add_order_by_to_selects(new_txt)
    return new_txt, issues

# -----------------------------
# API
# -----------------------------

@app.post("/remediate-mm-im")
async def remediate_mm_im(units: List[Unit]):
    """
    Input: list of ABAP 'units' with code.
    Output: same structure with appended 'mb_txn_usage' list of remediation suggestions.
    """
    results = []
    for u in units:
        src = u.original_code or ""
        # issues = find_mm_im_issues(src)
        remediated_src, issues = remediate_code(src)

        obj = json.loads(u.model_dump_json())
        # obj["mb_txn_usage"] = issues
        obj["remediated_code"] = remediated_src
        results.append(obj)
    return results
