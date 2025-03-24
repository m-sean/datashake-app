import enum
import mongoengine
import pydantic


class ScheduleFrequency(str, enum.Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class JobStatus(str, enum.Enum):
    COMPLETE = "complete"
    PENDING = "pending"
    INVALID_URL = "invalid_url"
    MAINTENANCE = "maintenance"
    FAILED = "failed"


class Product(pydantic.BaseModel):
    id: str
    brand: str
    format: str


class ScrapeParams(pydantic.BaseModel):
    url: str
    callback: str
    from_date: str | None = None
    diff: int | None = None


class ScheduleScrapeRequest(pydantic.BaseModel):
    frequency: ScheduleFrequency
    schedule_name: str = None
    params: ScrapeParams

    def validate(self):
        mongoengine.URLField().validate(self.params.url)
        if self.params.from_date:
            mongoengine.DateField().validate(self.params.from_date)
