from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel

from libs.constants.area import Area


class TodayForecast(BaseModel):
    # 本日の電力使用の見通し
    # 日時
    dt: datetime
    # 使用電力の当日実績
    actual_result: int
    # 使用電力の予測値
    forecast_demand: int
    # 供給電力の予測値
    forecast_supply: int

    @property
    def forecast_usage_pc(self):
        return int(self.forecast_demand / self.forecast_supply * 100)


class TomorrowForecast(BaseModel):
    # 翌日の電力使用の見通し
    date: date
    # 需要ピーク時
    demand_peak_time: str  # 18:00～19:00
    demand_peak_supply: int
    demand_peak_demand: int

    # 使用率ピーク時
    usage_peak_time: str
    usage_peak_supply: int
    usage_peak_demand: int

    # 気温
    temperature: float


class ForecastData(BaseModel):
    area: Area
    today_forecasts: List[TodayForecast]
    tomorrow_forecast: Optional[TomorrowForecast]
