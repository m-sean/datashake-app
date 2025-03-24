import mongoengine


class DatashakeSchedule(mongoengine.Document):
    schedule_id = mongoengine.IntField()
    url = mongoengine.URLField()
    disabled = mongoengine.BooleanField()


class ProductMapping(mongoengine.Document):
    product_id = mongoengine.StringField()
    brand = mongoengine.StringField()
    format = mongoengine.StringField()


class ProductReview(mongoengine.Document):
    datashake_review_uuid = mongoengine.StringField()
    scraper_review_id = mongoengine.IntField()
    source_review_id = mongoengine.StringField()
    product_id = mongoengine.StringField()
    brand = mongoengine.StringField(default="SKU_NOT_LISTED")
    format = mongoengine.StringField(default="SKU_NOT_LISTED")
    job_id = mongoengine.IntField()
    source_name = mongoengine.StringField(default="")
    source_url = mongoengine.URLField()
    author_name = mongoengine.StringField(default="")
    date = mongoengine.DateField()
    rating_value = mongoengine.FloatField()
    review_text = mongoengine.StringField(default="")
    review_url = mongoengine.StringField(default="")
    location = mongoengine.StringField()
    review_title = mongoengine.StringField(default="")
    verified_order = mongoengine.BooleanField()
    reviewer_title = mongoengine.StringField()
    language_code = mongoengine.StringField()
    profile_picture = mongoengine.StringField()
    meta_data = mongoengine.StringField()
    review_source = mongoengine.StringField(default="")
    response = mongoengine.DictField()
