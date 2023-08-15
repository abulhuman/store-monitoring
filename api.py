import psycopg2
from fastapi import FastAPI, APIRouter

from reporting import create_report

app = FastAPI()

api_router = APIRouter()

_connection = psycopg2.connect(
    host="localhost",
    database="loop_datastore_db",
    user="loop_datastore_admin",
    password="top_secret"
)

@api_router.get('/trigger_report')
def trigger_report():
    return {'report_id': create_report(_connection)}


@api_router.get('/get_report')
def get_report():
    return {'message': 'Report generated'}


app.include_router(api_router, prefix='/api/v1')
