from pydantic import BaseModel, Field


class AdCreate(BaseModel):
    adgroup_id: str
    ad_name: str
    ad_text: str | None = None
    image_ids: list[str] | None = None
    video_id: str | None = None
    call_to_action: str | None = None
    landing_page_url: str | None = None
    identity_id: str | None = None
    identity_type: str | None = None


class AdUpdate(BaseModel):
    ad_id: str
    ad_name: str | None = None
    ad_text: str | None = None
    operation_status: str | None = None
