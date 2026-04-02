from pydantic import BaseModel, Field


class AdGroupCreate(BaseModel):
    campaign_id: str
    adgroup_name: str
    placement_type: str = "PLACEMENT_TYPE_AUTOMATIC"
    billing_event: str = "OCPM"
    optimization_goal: str = "CLICK"
    bid_price: float | None = None
    conversion_bid_price: float | None = None
    budget_mode: str = "BUDGET_MODE_DAY"
    budget: float = 50.0
    schedule_type: str = "SCHEDULE_FROM_NOW"
    schedule_start_time: str | None = None
    schedule_end_time: str | None = None
    location_ids: list[str] = Field(default_factory=list)
    gender: str | None = None
    age_groups: list[str] | None = None
    languages: list[str] | None = None
    operating_systems: list[str] | None = None
    promotion_type: str | None = None
    app_id: str | None = None


class AdGroupUpdate(BaseModel):
    adgroup_id: str
    adgroup_name: str | None = None
    budget: float | None = None
    bid_price: float | None = None
    operation_status: str | None = None
