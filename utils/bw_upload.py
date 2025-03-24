import logging
import requests
from typing import Dict, Iterable, List

from models import ProductReview
from utils.decorators import retry, timeout
from utils.util import CONFIG, dedupe_data


_DEDUPE_ON = ["date", "review_text", "author_name", "review_source"]

class BrandwatchUploader:
    _BASE_ENDPOINT = "https://api.brandwatch.com"
    _LOGIN = f"{_BASE_ENDPOINT}/oauth/token"
    _UPLOAD = f"{_BASE_ENDPOINT}/content/upload"
    _SOURCES = f"{_BASE_ENDPOINT}/content/sources/list"

    _BW_FIELD_MAPPING = {
        "datashake_review_uuid": "guid",
        "date": "date",
        "review_text": "contents",
        "review_title": "title",
        "review_url": "url",
        "author_name": "author",
    }

    _BW_CUSTOM_FIELDS = {
        "brand": "brand",
        "datashake_review_uuid": "uuid",
        "format": "format",
        "product_id": "product_id",
        "rating_value": "rating",
        "review_source": "domain",
        "source_review_id": "review_id"
    }

    def __init__(self):
        self.access_token = self._login()
        self.header = {
            "Authorization": "Bearer {}".format(self.access_token),
            "content-type": "application/json",
        }
        self.data_sources = self.get_sources()

    def upload_data(self, data: Iterable[ProductReview], source_id: int):
        data = dedupe_data([row.to_mongo() for row in data], _DEDUPE_ON)
        all_responses = []
        for batch in _batch_iter(data):
            response = self._upload_batch(batch, source_id)
            all_responses.append(response)
        return all_responses

    @retry(target_exception=Exception, max_retries=3, max_backoff=5)
    @timeout(timeout_max=30)
    def _login(self) -> None:
        auth_params = {
            "username": CONFIG.get("brandwatch", "username"),
            "password": CONFIG.get("brandwatch", "password"),
            "grant_type": "partner-password",
            "client_id": "partner-api-client",
        }
        response = requests.get(self._LOGIN, params=auth_params)
        response.raise_for_status()
        return response.json()["access_token"]

    @retry(target_exception=Exception, max_retries=3, max_backoff=5)
    @timeout(timeout_max=30)
    def get_sources(self) -> Dict[str, int]:
        response = requests.get(self._SOURCES, headers=self.header)
        response.raise_for_status()
        return {src["name"]: src["id"] for src in response.json()["results"]}

    @retry(target_exception=Exception, max_retries=3, max_backoff=5)
    @timeout(timeout_max=30)
    def push_data(self, data: Dict[str, any]) -> Dict[str, any]:
        logging.info(f"Pushing {len(data['items'])} documents to Brandwatch")
        response = requests.post(self._UPLOAD, json=data, headers=self.header)
        response.raise_for_status()
        return response.json()

    def as_bw_mention(self, source_row: Dict[str, any]) -> Dict[str, any]:
        mention = {}
        for src, target in self._BW_FIELD_MAPPING.items():
            if (value := source_row.get(src)) is not None:
                if target == "url" and not value:
                    value = source_row.get("source_url")
                mention[target] = value
        custom = {}
        for src, target in self._BW_CUSTOM_FIELDS.items():
            if (value := source_row.get(src)) is not None and value != "":
                custom[target] = value
        if custom:
            mention["custom"] = custom
        return mention

    def _upload_batch(self, batch_rows: List[dict], source_id: int):
        data = {"items": [], "contentSource": source_id}
        for row in batch_rows:
            mention = self.as_bw_mention(row)
            data["items"].append(mention)
        response = self.push_data(data=data)
        return response


def _validated_row(row: Dict[str, any]):
    if not (row["review_text"].strip()):
        if not (title := row["review_title"].strip()):
            if row["rating"] is None:
                return
            row["review_text"] = str(row["rating_value"])
        else:
            row["review_text"] = title
    # Truncate if above BW limits
    if len(row["review_text"]) >= 16000:
        row["review_text"] = row["review_text"][:15990]
    if len(row["review_title"]) >= 199:
        row["review_title"] = row["review_title"][:190]
    return row


def _batch_iter(all_rows: Iterable[dict], batch_size=1000):
    batch = []
    for row in all_rows:
        if row := _validated_row(row):
            batch.append(row)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
