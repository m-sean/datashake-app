import mongoengine
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Response
from models import DatashakeSchedule
from serializers import ScheduleScrapeRequest, Product
from tasks import (
    process_callback,
    process_create_schedule,
    process_delete_schedule,
    add_products,
    check_for_maintenance_jobs,
)
from utils.util import notify, CONFIG, validate_api_key

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = BackgroundScheduler()
    scheduler.add_job(check_for_maintenance_jobs, "interval", hours=6)
    # scheduler.add_job(push_data, "cron", )
    scheduler.start()
    yield

app = FastAPI(lifespan=lifespan)
router = APIRouter()
db = mongoengine.connect(
    db=CONFIG.get("mongo_db", "database"),
    host=CONFIG.get("mongo_db", "host"),
    port=CONFIG.getint("mongo_db", "port"),
)

@router.post("/process_job")
def process_job(request: dict):
    try:
        job_id = request["job_id"]
        status = request["crawl_status"]
        process_callback(job_id, status)
    except KeyError as err:
        notify(f"Received an unexpected callback:\n{request}")
        raise HTTPException(
            status_code=400, detail=f"Request did not containt the required field(s)."
        )
    except Exception as err:
        raise err
    return Response(status_code=200)


@router.post("/schedule", dependencies=[Depends(validate_api_key)])
def create_schedule(request: ScheduleScrapeRequest):
    try:
        request.validate()
    except mongoengine.ValidationError as err:
        raise HTTPException(status_code=400, detail=err)
    if DatashakeSchedule.objects.filter(url=request.params.url):
        raise HTTPException(
            status_code=400, detail="A schedule already exists for this URL."
        )
    response = process_create_schedule(
        request.frequency, request.schedule_name, request.params
    )
    if response["status"] != "success":
        raise HTTPException(400, detail=response)
    else:
        data = response["results"][0]
        schedule = DatashakeSchedule(
            schedule_id=data["schedule_id"], url=data["payload"]["query_params"]["url"]
        )
        schedule.save()
    return Response(status_code=201)


@router.delete("/schedule", dependencies=[Depends(validate_api_key)])
def delete_schedule(schedule_id: int):
    process_delete_schedule(schedule_id)
    return Response(status_code=204)


@router.post("/product_mapping", dependencies=[Depends(validate_api_key)])
def update_product_mapping(products: list[Product]):
    skipped = add_products(products)
    if skipped:
        return Response(
            status_code=207,
            content=f"The following were skipped because their ids already exist: {skipped}",
        )
    return Response(status_code=201)

app.include_router(router)
