from io import StringIO
from uuid import uuid4
import psycopg2
from fastapi import FastAPI, APIRouter
from starlette.background import BackgroundTasks
from starlette.responses import JSONResponse, Response

from reporting import create_report, generate_report_data, is_report_completed, get_report_data, convert_to_csv

app = FastAPI()

api_router = APIRouter()

_connection = psycopg2.connect(
    host="localhost",
    database="loop_datastore_db",
    user="loop_datastore_admin",
    password="top_secret"
)


@api_router.get('/trigger_report')
def trigger_report(background_tasks: BackgroundTasks):
    """
    Triggers the creation and generation of a report in the background.

    :param background_tasks: The background task manager.

    :return: A JSON response containing the report ID and a status code of 201.
    """
    report_id = str(uuid4())
    background_tasks.add_task(create_report, _connection, report_id)
    background_tasks.add_task(generate_report_data, _connection, report_id)
    return JSONResponse(status_code=201, content={"report_id": report_id}, background=background_tasks)


@api_router.get('/get_report/{report_id}')
def get_report(report_id: str):
    """
    Retrieves a report with the given ID from the database.

    :param report_id: The ID of the report to retrieve.

    return: The report with the given ID, or None if no such report exists.
    """
    is_completed = is_report_completed(_connection, report_id)
    if is_completed is None:
        return JSONResponse(status_code=404, content={"error": "Report not found."})
    if not is_completed:
        return Response(status_code=200, content="Running")
    report_data = get_report_data(_connection, report_id)
    csv_string = convert_to_csv(report_data)
    csv_file = StringIO(csv_string)
    return Response(content=csv_file.getvalue(), media_type="text/csv")


app.include_router(api_router, prefix='/api/v1')
