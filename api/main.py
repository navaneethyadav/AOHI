"""
main.py - FastAPI app that exposes / and /report_pro endpoints.

Usage:
    uvicorn api.main:app --reload --port 8000
"""
from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import logging

from .generate_report_pro import generate_report

logger = logging.getLogger("aohi")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

app = FastAPI(title="AOHI Report API")

# Optional: enable CORS for local testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def home():
    return {"message": "AOHI API is running. Try GET /report_pro"}


@app.get("/report_pro")
def report_pro(force: bool = False):
    """
    Generate and return a PDF file.
    Query params:
      - force (bool): unused in demo; kept for compatibility with your previous client.
    Returns:
      - application/pdf response with attachment header
    """
    try:
        pdf_buffer = generate_report()
        return Response(
            content=pdf_buffer.read(),
            media_type="application/pdf",
            headers={"Content-Disposition": "attachment; filename=AOHI_Report.pdf"},
        )
    except Exception as exc:
        # Return a helpful JSON error that includes the error string (not the full traceback)
        logger.exception("Error in /report_pro endpoint")
        raise HTTPException(status_code=500, detail=f"Report generator failed: {str(exc)}")