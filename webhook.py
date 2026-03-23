import os
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import Response
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables (Database URL)
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not set in .env")

# Initialize database engine
engine = create_engine(DATABASE_URL)

# Initialize FastAPI App
app = FastAPI(
    title="Marketplaces Export Webhook",
    description="API webhook to export sliced CSV data from the unified database.",
    version="1.0"
)

@app.get("/export/csv")
def export_csv(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)", example="2026-03-20"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)", example="2026-03-25"),
    limit: Optional[int] = Query(None, description="Max number of rows to return", example=1000)
):
    """
    Downloads the database as a CSV file.
    Optional query parameters allow slicing the data by scraped date range and/or row limit.
    """
    # Build generic cross-platform base query
    base_sql = "SELECT * FROM publishers_v2 WHERE 1=1"
    params = {}
    
    # Append Date Filters securely mapped via SQLAlchemy Text params
    if start_date:
        base_sql += " AND updated_at >= :start_date"
        params['start_date'] = f"{start_date} 00:00:00"
        
    if end_date:
        base_sql += " AND updated_at <= :end_date"
        params['end_date'] = f"{end_date} 23:59:59"
        
    # Append Limiter explicitly casted to integer for SQL injection safety
    if limit is not None and limit > 0:
        base_sql += f" LIMIT {int(limit)}"
        
    try:
        # Utilize Panda's read_sql wrapper for effortless query-to-CSV generation
        query = text(base_sql)
        df = pd.read_sql(query, engine, params=params)
        
        # Convert DataFrame to a string-based CSV Payload
        csv_data = df.to_csv(index=False)
        
        # Format the Download Header Identity
        filename = f"marketplaces_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        headers = {
            'Content-Disposition': f'attachment; filename="{filename}"'
        }
        
        return Response(content=csv_data, media_type="text/csv", headers=headers)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database Export Failed: {str(e)}")

@app.get("/logs")
def view_logs(
    lines: Optional[int] = Query(500, description="Number of tail lines to display", example=100)
):
    """
    Outputs the backend orchestrator system logs dynamically into plaintext.
    """
    # The Log file resides in the mounted docker folder
    log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "system.log")
    
    if not os.path.exists(log_file):
        raise HTTPException(status_code=404, detail="System log file has not been initialized yet.")
        
    try:
        with open(log_file, "r") as f:
            all_lines = f.readlines()
            
        # Return only the recent tail history requested
        output_lines = "".join(all_lines[-lines:]) if lines else "".join(all_lines)
        return Response(content=output_lines, media_type="text/plain")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed reading logs: {str(e)}")

# If run directly via `python3 webhook.py`
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
