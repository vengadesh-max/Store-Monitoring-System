import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Dict

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from task.database import DatabaseManager
from task.report_generator import ReportGenerator

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


report_status: Dict[str, str] = {}
report_results: Dict[str, str] = {}

db_manager = DatabaseManager()
report_gen = ReportGenerator(db_manager)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database and load data on startup"""
    await db_manager.initialize_database()
    await db_manager.load_data_from_csv()
    yield


app = FastAPI(title="Store Monitoring System", version="1.0.0", lifespan=lifespan)


@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Store Monitoring System API"}


@app.post("/trigger_report")
async def trigger_report():
    """Trigger report generation from stored data"""
    report_id = str(uuid.uuid4())
    report_status[report_id] = "Running"
    asyncio.create_task(generate_report_async(report_id))
    return {"report_id": report_id}


@app.get("/get_report/{report_id}")
async def get_report(report_id: str):
    """Get report status or download completed CSV file"""
    if report_id not in report_status:
        raise HTTPException(status_code=404, detail="Report ID not found")

    status = report_status[report_id]

    if status == "Running":
        return {"status": "Running"}
    elif status == "Complete":
        if report_id in report_results:
            csv_file_path = report_results[report_id]
            if os.path.exists(csv_file_path):
                return FileResponse(
                    path=csv_file_path,
                    media_type="text/csv",
                    filename=f"store_report_{report_id}.csv",
                )
            else:
                raise HTTPException(
                    status_code=500, detail="Report completed but file not found"
                )
        else:
            raise HTTPException(
                status_code=500, detail="Report completed but file not found"
            )
    else:
        raise HTTPException(status_code=500, detail=f"Unknown report status: {status}")


async def generate_report_async(report_id: str):
    """Generate report asynchronously"""
    try:
        csv_file_path = await report_gen.generate_report(report_id)
        report_status[report_id] = "Complete"
        report_results[report_id] = csv_file_path
    except Exception as e:
        report_status[report_id] = "Failed"
        logger.error(f"Error generating report {report_id}: {str(e)}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
