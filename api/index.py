import csv
import io
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from mangum import Mangum

app = FastAPI(title="Instructor Monitor API")

# CORS so the static index.html can call this from any origin (localhost dev + Vercel prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["*"],
)

# Column aliases — matches the CSV produced by exportCSV() in index.html
COLUMN_MAP = {
    "instructor_email": "incoming",
    "super_batch_name": "batch",
    "super_batch":      "batch",       # alternate short header
    "module_name":      "module",
    "last_class_taken_at": "lastClass",
    "last_class":          "lastClass",  # alternate short header
    "first_class_taken_at": "firstClass",
    "first_class":          "firstClass", # alternate short header
    "prev_module_instructor": "prev",
    "prev":                   "prev",      # alternate short header
}


def normalise_header(h: str) -> str:
    """Lower-case, strip quotes/whitespace, then map to our canonical key."""
    cleaned = h.strip().strip('"').lower()
    # Resolve via the alias map (try full match then partial)
    if cleaned in COLUMN_MAP:
        return COLUMN_MAP[cleaned]
    for alias, canonical in COLUMN_MAP.items():
        if alias in cleaned:
            return canonical
    return cleaned  # keep as-is if unknown


@app.post("/api/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted.")

    try:
        contents = await file.read()
        text = contents.decode("utf-8-sig")  # handle BOM if present
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not read file: {e}")

    reader = csv.DictReader(io.StringIO(text))

    if reader.fieldnames is None:
        raise HTTPException(status_code=400, detail="CSV has no headers.")

    # Build a normalised header → original header mapping
    header_mapping: dict[str, str] = {
        orig: normalise_header(orig) for orig in reader.fieldnames
    }

    rows = []
    for raw_row in reader:
        # Re-key each row using our normalised names
        row: dict[str, str] = {}
        for orig_key, value in raw_row.items():
            if orig_key is None:
                continue
            canonical = header_mapping.get(orig_key, orig_key)
            row[canonical] = (value or "").strip().strip('"')

        # Skip rows with no outgoing instructor — these aren't change events
        if not row.get("prev"):
            continue

        rows.append({
            "batch":      row.get("batch", ""),
            "module":     row.get("module", ""),
            "prev":       row.get("prev", ""),
            "incoming":   row.get("incoming", ""),
            "firstClass": _normalise_date(row.get("firstClass", "")),
            "lastClass":  _normalise_date(row.get("lastClass", "")),
        })

    return JSONResponse(content={"rows": rows, "total": len(rows)})


def _normalise_date(value: str) -> str:
    """Return YYYY-MM-DD if parseable, otherwise return value as-is."""
    if not value:
        return ""
    from datetime import datetime
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Last resort: try ISO parsing (handles timestamps like 2026-04-27T00:00:00)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except ValueError:
        return value


# Vercel / AWS Lambda handler
handler = Mangum(app)
