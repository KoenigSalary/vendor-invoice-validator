# utils/smart_reader.py
from __future__ import annotations
import os, io, re, json
import pandas as pd
import chardet

# --- Light "magic number" sniffers ---
OLE_SIGNATURE = b"\xD0\xCF\x11\xE0"  # legacy Excel .xls (BIFF in OLE)
ZIP_SIGNATURE = b"PK\x03\x04"        # .xlsx/.xlsm/.zip containers
PDF_SIGNATURE = b"%PDF"
HTML_PATTERN = re.compile(br"(?is)\s*<!DOCTYPE|<html|<table")

def _read_text_like(buf: bytes) -> pd.DataFrame | None:
    """
    Try HTML → CSV/TSV/; → JSON array of objects → NDJSON
    """
    # 1) HTML table(s)
    if HTML_PATTERN.search(buf):
        try:
            text = buf.decode("utf-8", errors="replace")
            tables = pd.read_html(io.StringIO(text))
            if tables:
                return pd.concat(tables, ignore_index=True)
        except Exception:
            pass

    # 2) Encoding detect
    enc = chardet.detect(buf or b"").get("encoding") or "utf-8"
    text = buf.decode(enc, errors="replace")

    # 3) CSV / TSV / semicolon
    for sep in ("\t", ",", ";", "|"):
        try:
            df = pd.read_csv(io.StringIO(text), sep=sep)
            if df.shape[1] > 1 or df.shape[0] > 1:
                return df
        except Exception:
            pass

    # 4) JSON array of objects
    try:
        obj = json.loads(text)
        if isinstance(obj, list) and obj and isinstance(obj[0], dict):
            return pd.DataFrame(obj)
    except Exception:
        pass

    # 5) NDJSON
    try:
        rows = [json.loads(line) for line in text.splitlines() if line.strip()]
        if rows and isinstance(rows[0], dict):
            return pd.DataFrame(rows)
    except Exception:
        pass

    return None


def _read_excel_by_signature(path: str, head: bytes) -> pd.DataFrame | None:
    # True legacy .xls
    if head.startswith(OLE_SIGNATURE):
        try:
            return pd.read_excel(path, engine="xlrd")
        except Exception:
            return None
    # .xlsx/.xlsm (zip)
    if head.startswith(ZIP_SIGNATURE):
        try:
            return pd.read_excel(path, engine="openpyxl")
        except Exception:
            return None
    return None


def _read_pdf_minimal(path: str) -> pd.DataFrame | None:
    """
    Minimal PDF support (no heavy deps): extract text via PyMuPDF and
    try to coerce table-like lines. Best-effort so validation can still run.
    """
    try:
        import fitz  # PyMuPDF
    except Exception:
        return None

    try:
        doc = fitz.open(path)
        lines = []
        for page in doc:
            text = page.get_text("text")
            for ln in text.splitlines():
                # Split on common delimiters
                parts = re.split(r"\s{2,}|\t|,|;", ln.strip())
                parts = [p.strip() for p in parts if p.strip()]
                if len(parts) >= 2:
                    lines.append(parts)
        doc.close()
        if not lines:
            return None

        # Heuristic: use the longest line as header if it looks like labels
        header = max(lines[:20], key=len)
        # Ensure header uniqueness
        header = [f"col_{i}" if not h or h.isnumeric() else h for i, h in enumerate(header)]
        rows = [r for r in lines if len(r) == len(header)]
        if not rows:
            return None
        return pd.DataFrame(rows, columns=header)
    except Exception:
        return None


def read_any_table(path: str) -> pd.DataFrame:
    """
    Robust reader that ignores extension and decides by content:
    - Real Excel: .xls (OLE) / .xlsx (ZIP)
    - HTML “fake .xls”
    - CSV/TSV/; delimited
    - JSON array / NDJSON
    - Minimal PDF fallback (PyMuPDF)
    - Last resort: let pandas try read_excel / read_csv
    """
    with open(path, "rb") as f:
        head = f.read(8)
        f.seek(0)
        raw = f.read()

    # 1) Excel by signature
    df = _read_excel_by_signature(path, head)
    if df is not None:
        return df

    # 2) Text-like (HTML/CSV/TSV/JSON/NDJSON)
    df = _read_text_like(raw)
    if df is not None:
        return df

    # 3) PDF minimal
    if head.startswith(PDF_SIGNATURE):
        df = _read_pdf_minimal(path)
        if df is not None:
            return df

    # 4) Last resorts
    for attempt in (
        lambda: pd.read_excel(path, engine="openpyxl"),
        lambda: pd.read_csv(path),
    ):
        try:
            return attempt()
        except Exception:
            pass

    raise ValueError(f"Unsupported or unreadable file: {os.path.basename(path)}")
