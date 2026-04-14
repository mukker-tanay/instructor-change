import csv
import io
import json
import os
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from mangum import Mangum

app = FastAPI(title="Instructor Monitor API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Column alias map — handles both the CSV export format and the Google Sheet
# headers in the "dump" tab.
#
# Sheet headers: instructor_email | super_batch_name | module_name |
#                classes_taken | last_class_taken_at | first_class_taken_at |
#                rnk | prev_module_instructor
# ---------------------------------------------------------------------------
COLUMN_MAP = {
    "instructor_email":       "incoming",
    "super_batch_name":       "batch",
    "super_batch":            "batch",
    "module_name":            "module",
    "last_class_taken_at":    "lastClass",
    "last_class":             "lastClass",
    "first_class_taken_at":   "firstClass",
    "first_class":            "firstClass",
    "prev_module_instructor": "prev",
    "prev":                   "prev",
    # ignored: classes_taken, rnk
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalise_header(h: str) -> str:
    cleaned = h.strip().strip('"').lower()
    if cleaned in COLUMN_MAP:
        return COLUMN_MAP[cleaned]
    for alias, canonical in COLUMN_MAP.items():
        if alias in cleaned:
            return canonical
    return cleaned


def _normalise_date(value: str) -> str:
    if not value:
        return ""
    from datetime import datetime
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except ValueError:
        return value


def _get_supabase():
    from supabase import create_client
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        raise HTTPException(status_code=500, detail="Supabase env vars not configured.")
    return create_client(url, key)


def _get_gspread_client():
    import gspread
    from google.oauth2.service_account import Credentials
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not raw:
        raise HTTPException(status_code=500, detail="GOOGLE_CREDENTIALS_JSON env var not set.")
    try:
        creds_dict = json.loads(raw)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"GOOGLE_CREDENTIALS_JSON is not valid JSON: {e}")
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    try:
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build credentials from service account JSON: {e}")
    try:
        return gspread.authorize(creds)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"gspread.authorize failed: {e}")


def _rows_to_db_records(rows: list[dict]) -> list[dict]:
    """Convert frontend-shaped rows to Supabase table columns."""
    records = []
    for r in rows:
        first = _normalise_date(r.get("firstClass", "")) or None
        last  = _normalise_date(r.get("lastClass", ""))  or None
        if not r.get("prev"):
            continue
        records.append({
            "batch":               r.get("batch", ""),
            "module":              r.get("module", ""),
            "prev_instructor":     r.get("prev", ""),
            "incoming_instructor": r.get("incoming", ""),
            "first_class":         first,
            "last_class":          last,
        })
    return records


# ---------------------------------------------------------------------------
# POST /api/upload-csv  — parse CSV, persist to Supabase, return rows
# ---------------------------------------------------------------------------
@app.post("/api/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted.")

    try:
        contents = await file.read()
        text = contents.decode("utf-8-sig")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read file: {e}")

    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise HTTPException(status_code=400, detail="CSV has no headers.")

    header_mapping = {orig: normalise_header(orig) for orig in reader.fieldnames}

    rows = []
    for raw_row in reader:
        row: dict[str, str] = {}
        for orig_key, value in raw_row.items():
            if orig_key is None:
                continue
            row[header_mapping.get(orig_key, orig_key)] = (value or "").strip().strip('"')

        prev     = row.get("prev", "")
        incoming = row.get("incoming", "")

        # Skip rows where outgoing == incoming — not a real instructor change
        if not prev or prev.strip().lower() == incoming.strip().lower():
            continue

        rows.append({
            "batch":      row.get("batch", ""),
            "module":     row.get("module", ""),
            "prev":       prev,
            "incoming":   incoming,
            "firstClass": _normalise_date(row.get("firstClass", "")),
            "lastClass":  _normalise_date(row.get("lastClass", "")),
        })

    # Persist to Supabase (best-effort — don't fail the call if Supabase is unconfigured)
    try:
        sb = _get_supabase()
        records = _rows_to_db_records(rows)
        if records:
            sb.table("instructor_changes").upsert(
                records,
                on_conflict="batch,module,prev_instructor,incoming_instructor,first_class"
            ).execute()
    except HTTPException:
        pass  # Supabase not configured — still return the parsed rows

    return JSONResponse(content={"rows": rows, "total": len(rows)})


# ---------------------------------------------------------------------------
# POST /api/sync  — pull from Google Sheet "dump" tab → upsert Supabase
# ---------------------------------------------------------------------------
@app.post("/api/sync")
async def sync_from_sheet():
    sheet_id = os.environ.get("GOOGLE_SHEET_ID", "")
    if not sheet_id:
        raise HTTPException(status_code=500, detail="GOOGLE_SHEET_ID env var not set.")

    try:
        gc = _get_gspread_client()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to create Google Sheets client: {e}")

    try:
        sh = gc.open_by_key(sheet_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to open sheet '{sheet_id}'. Is the sheet shared with the service account? Error: {e}")

    try:
        worksheet = sh.worksheet("Data")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Tab 'dump' not found in the sheet. Available tabs: {[ws.title for ws in sh.worksheets()]}. Error: {e}")

    try:
        records = worksheet.get_all_records()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to read rows from 'dump' tab: {e}")

    rows = []
    for record in records:
        def g(key: str) -> str:
            return str(record.get(key, "") or "").strip()

        prev     = g("prev_module_instructor")
        incoming = g("instructor_email")

        # Skip rows where outgoing == incoming — not a real instructor change
        if not prev or prev.strip().lower() == incoming.strip().lower():
            continue

        rows.append({
            "batch":               g("super_batch_name"),
            "module":              g("module_name"),
            "prev_instructor":     prev,
            "incoming_instructor": incoming,
            "first_class":         _normalise_date(g("first_class_taken_at")) or None,
            "last_class":          _normalise_date(g("last_class_taken_at"))  or None,
        })

    if not rows:
        return JSONResponse(content={"synced": 0, "message": "No rows with prev_module_instructor found."})

    try:
        sb = _get_supabase()
        sb.table("instructor_changes").upsert(
            rows,
            on_conflict="batch,module,prev_instructor,incoming_instructor,first_class"
        ).execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Supabase upsert failed: {e}")

    return JSONResponse(content={"synced": len(rows)})


# ---------------------------------------------------------------------------
# GET /api/rows  — read all rows from Supabase, return in frontend shape
# ---------------------------------------------------------------------------
@app.get("/api/rows")
async def get_rows():
    try:
        sb = _get_supabase()
        result = sb.table("instructor_changes").select("*").execute()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Supabase read failed: {e}")

    rows = [
        {
            "batch":      r.get("batch", ""),
            "module":     r.get("module", ""),
            "prev":       r.get("prev_instructor", ""),
            "incoming":   r.get("incoming_instructor", ""),
            "firstClass": r.get("first_class", "") or "",
            "lastClass":  r.get("last_class", "")  or "",
        }
        for r in result.data
    ]

    return JSONResponse(content={"rows": rows, "total": len(rows)})


# ---------------------------------------------------------------------------
# GET /api/debug  — check which env vars are configured (no secrets exposed)
# ---------------------------------------------------------------------------
@app.get("/api/debug")
async def debug_config():
    checks = {
        "SUPABASE_URL":           bool(os.environ.get("SUPABASE_URL")),
        "SUPABASE_SERVICE_KEY":   bool(os.environ.get("SUPABASE_SERVICE_KEY")),
        "GOOGLE_SHEET_ID":        bool(os.environ.get("GOOGLE_SHEET_ID")),
        "GOOGLE_CREDENTIALS_JSON": bool(os.environ.get("GOOGLE_CREDENTIALS_JSON")),
    }

    # Try to parse the Google creds JSON
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if raw:
        try:
            parsed = json.loads(raw)
            checks["GOOGLE_CREDS_client_email"] = parsed.get("client_email", "MISSING")
            checks["GOOGLE_CREDS_has_private_key"] = bool(parsed.get("private_key"))
        except Exception as e:
            checks["GOOGLE_CREDS_parse_error"] = str(e)

    checks["GOOGLE_SHEET_ID_value"] = os.environ.get("GOOGLE_SHEET_ID", "")

    return JSONResponse(content=checks)


# ---------------------------------------------------------------------------
# Vercel / Lambda entrypoint
# ---------------------------------------------------------------------------
handler = Mangum(app)
