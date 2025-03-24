import json
import math
import requests
from datetime import date
from typing import List, Optional

from models import (
    DatashakeSchedule,
    ProductMapping,
    ProductReview,
)
from serializers import JobStatus, Product, ScheduleFrequency, ScrapeParams
from utils.bw_upload import BrandwatchUploader
from utils.decorators import retry, timeout
from utils.util import CONFIG, write_to_google_sheet, notify


_SCHEDULES = CONFIG.get("datashake", "schedule_endpoint")
_PROFILES = CONFIG.get("datashake", "profiles_endpoint")
_JOBS = f"{_PROFILES}/jobs"
_INFO = f"{_PROFILES}/info"
_REVIEWS = f"{_PROFILES}/reviews"
_COUNT_KEYS = {
    _JOBS: "total",
    _REVIEWS: "result_count",
}
_HEADERS = {
    "spiderman-token": CONFIG.get("datashake", "access_token"),
    "content-type": "application/json",
}


@retry(Exception, 3, 5)
@timeout(30)
def process_create_schedule(
    frequency: ScheduleFrequency,
    query_params: ScrapeParams,
    schedule_name: Optional[str] = None,
):
    headers = {
        "x-api-key": _HEADERS["spiderman-token"],
        "content-type": _HEADERS["content-type"],
    }
    params = {
        "service": "rsapi",
        "endpoint": "add",
        "frequency": frequency,
        "schedule_name": schedule_name,
        "query_params": query_params.model_dump(),
    }
    response = requests.post(
        url=_SCHEDULES,
        headers=headers,
        json=params,
    )
    response.raise_for_status()
    return response.json()


@retry(Exception, 3, 5)
@timeout(30)
def process_delete_schedule(schedule_id):
    response = requests.delete(
        url=f"{_SCHEDULES}/{schedule_id}",
        headers=_HEADERS,
    )
    response.raise_for_status()
    return response.json()


def process_callback(job_id: int, status: JobStatus):
    try:
        if status != JobStatus.COMPLETE:
            job_info = _get_info(job_id)
            url = job_info["url"]
            message = f"Job ID: {job_id}\n" f"Status: {status}\n" f"URL: {url}"
            notify(message)
            if status == JobStatus.INVALID_URL:
                for schedule in DatashakeSchedule.objects.filter(url=url):
                    _disable_schedule(schedule.schedule_id)
                    schedule.disabled = True
                    schedule.save()
            return {}
        job_data = _get_job_reviews(job_id)
        save_data = []
        for review in _iter_job_reviews(job_data):
            review_json = json.dumps(review)
            product_review = ProductReview.from_json(review_json)
            save_data.append(product_review)
        for review in save_data:
            review.save()
    except Exception as err:
        notify(f"Unable to retrieve rewiews from job {job_id}.\nERROR: {err}")


def add_products(products: List[Product]):
    exists = []
    for prod in products:
        if ProductMapping.objects.filter(product_id=prod.id):
            exists.append(prod)
        else:
            ProductMapping(
                product_id=prod.id, brand=prod.brand, format=prod.format
            ).save()
    return exists


def check_for_maintenance_jobs():
    maintenance_jobs = _get_jobs(crawl_status="maintenance")
    if maintenance_jobs:
        notify(
            f"WARNING: {len(maintenance_jobs)} jobs are currently in maintenance status."
        )

def push_to_brandwatch(all_reviews):
    bw_uploader = BrandwatchUploader()
    source_id = bw_uploader.data_sources[CONFIG.get("brandwatch", "upload_source_name")]
    bw_uploader.upload_data(all_reviews, source_id=source_id)

def push_to_google_sheet(all_reviews):
    columns = [
        str(k) for k in all_reviews.first().to_mongo().keys() if k != "_id"
    ]
    data = [columns]
    for row in all_reviews:
        data.append([_get_ser_value(row, col) for col in columns])
    write_to_google_sheet(data, date.today().isoformat())


def push_data():
    all_reviews = ProductReview.objects.all()
    if all_reviews:
        try:
            push_to_brandwatch(all_reviews)
            push_to_google_sheet(all_reviews)
            all_reviews.delete()
        except Exception as err:
            notify(f"An error occured when trying to push the data: {err}")


@retry(Exception, 3, 5)
@timeout(60)
def _disable_schedule(schedule_id):
    response = requests.patch(
        url=f"{_SCHEDULES}/{schedule_id}", headers=_HEADERS, params={"disabled": True}
    )
    response.raise_for_status()
    return response.json()


@retry(Exception, 3, 5)
@timeout(60)
def _get_info(job_id: int):
    response = requests.get(
        url=_INFO,
        headers=_HEADERS,
        params={"job_id": job_id},
    )
    response.raise_for_status()
    return response.json()


@retry(Exception, 3, 5)
@timeout(60)
def _get_page(endpoint, base_params, page):
    response = requests.get(
        url=endpoint,
        headers=_HEADERS,
        params={**base_params, "page": page},
    )
    response.raise_for_status()
    return response.json()


def _iter_pages(endpoint: str, per_page: int, **query_params):
    current_page = 1
    base_params = {"per_page": per_page, **query_params}
    data = _get_page(endpoint, base_params, current_page)
    yield data
    result_count = data[_COUNT_KEYS[endpoint]]
    if result_count > per_page:
        page_ct = math.ceil(result_count / per_page)
        while current_page < page_ct:
            current_page += 1
            yield _get_page(endpoint, base_params, current_page)


def _get_job_reviews(job_id):
    review_pages = _iter_pages(_REVIEWS, per_page=500, job_id=job_id)
    all_reviews = next(review_pages)
    for page in review_pages:
        all_reviews["reviews"].extend(page["reviews"])
    return all_reviews


def _get_jobs(**query_params):
    found_jobs = []
    for page in _iter_pages(_JOBS, per_page=500, **query_params):
        found_jobs.extend(page["jobs"])
    return found_jobs


def _iter_job_reviews(job_data):
    product_id = job_data.pop("unique_id")
    row_data = {
        "job_id": job_data["job_id"],
        "source_url": job_data["source_url"],
        "source_name": job_data["source_name"],
        "product_id": product_id,
    }
    if mapping := ProductMapping.objects.filter(product_id=product_id).first():
        row_data["brand"] = mapping.brand
        row_data["format"] = mapping.format
    for review in job_data["reviews"]:
        review["scraper_review_id"] = review.pop("id")
        review["source_review_id"] = review.pop("unique_id")
        review["author_name"] = review.pop("name")
        review["review_url"] = review.pop("url")
        yield {**row_data, **review}


def _get_ser_value(obj, name):
    value = getattr(obj, name)
    if isinstance(value, date):
        return value.isoformat()
    elif isinstance(value, dict):
        return json.dumps(value)
    return value
