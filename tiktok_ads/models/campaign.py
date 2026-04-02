from pydantic import BaseModel, Field
from enum import Enum


class ObjectiveType(str, Enum):
    REACH = "REACH"
    TRAFFIC = "TRAFFIC"
    VIDEO_VIEWS = "VIDEO_VIEWS"
    LEAD_GENERATION = "LEAD_GENERATION"
    APP_PROMOTION = "APP_PROMOTION"
    WEB_CONVERSIONS = "WEB_CONVERSIONS"
    PRODUCT_SALES = "PRODUCT_SALES"


class BudgetMode(str, Enum):
    BUDGET_MODE_INFINITE = "BUDGET_MODE_INFINITE"
    BUDGET_MODE_DAY = "BUDGET_MODE_DAY"
    BUDGET_MODE_TOTAL = "BUDGET_MODE_TOTAL"


class CampaignCreate(BaseModel):
    campaign_name: str
    objective_type: ObjectiveType
    budget_mode: BudgetMode = BudgetMode.BUDGET_MODE_INFINITE
    budget: float | None = None
    operation_status: str = "ENABLE"
    campaign_type: str = "REGULAR_CAMPAIGN"
    app_promotion_type: str | None = None


class CampaignUpdate(BaseModel):
    campaign_id: str
    campaign_name: str | None = None
    budget_mode: BudgetMode | None = None
    budget: float | None = None
    operation_status: str | None = None
