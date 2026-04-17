from fastapi import APIRouter, Query
from meta_ads.api.reports import MetaReportService

router = APIRouter(prefix="/meta/reports", tags=["Meta 报表"])


@router.get("/insights")
async def account_insights(
    start_date: str = Query(...),
    end_date: str = Query(...),
    level: str = Query("campaign"),
    ad_account_id: str | None = Query(None),
    limit: int = Query(50),
):
    data = await MetaReportService(ad_account_id).get_account_insights(
        start_date=start_date, end_date=end_date, level=level, limit=limit,
    )
    return {"data": data.get("data", [])}
