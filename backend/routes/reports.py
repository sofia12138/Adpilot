from fastapi import APIRouter, Query
from tiktok_ads.api.report import ReportService

router = APIRouter(prefix="/reports", tags=["报表"])


@router.get("/campaign")
async def campaign_report(start_date: str = Query(...), end_date: str = Query(...), advertiser_id: str | None = Query(None), page: int = Query(1), page_size: int = Query(20)):
    return {"data": await ReportService(advertiser_id).get_campaign_report(start_date=start_date, end_date=end_date, page=page, page_size=page_size)}


@router.get("/adgroup")
async def adgroup_report(start_date: str = Query(...), end_date: str = Query(...), advertiser_id: str | None = Query(None), page: int = Query(1), page_size: int = Query(20)):
    return {"data": await ReportService(advertiser_id).get_adgroup_report(start_date=start_date, end_date=end_date, page=page, page_size=page_size)}


@router.get("/ad")
async def ad_report(start_date: str = Query(...), end_date: str = Query(...), advertiser_id: str | None = Query(None), page: int = Query(1), page_size: int = Query(20)):
    return {"data": await ReportService(advertiser_id).get_ad_report(start_date=start_date, end_date=end_date, page=page, page_size=page_size)}


@router.get("/audience")
async def audience_report(start_date: str = Query(...), end_date: str = Query(...), advertiser_id: str | None = Query(None), dimensions: str = Query("age,gender")):
    dims = [d.strip() for d in dimensions.split(",")]
    return {"data": await ReportService(advertiser_id).get_audience_report(start_date=start_date, end_date=end_date, dimensions=dims)}
