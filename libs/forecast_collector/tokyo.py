import csv
from datetime import date, datetime, timedelta
from io import StringIO

from libs.constants.area import Area
from libs.data.forecast import ForecastData, TodayForecast, TomorrowForecast
from libs.forecast_collector.base import Collector, DataDownloader, DataTransformer


class TokyoDataDownloader(DataDownloader):
    URL = "https://www.tepco.co.jp/forecast/html/images/juyo-s1-j.csv"

    def run(self) -> str:
        return self._fetch(url=self.URL)


class TodayForecastTransformer:
    FORECAST_HEADER = "DATE,TIME,当日実績(万kW),需要電力予測値(万kW),供給力予測値(万kW),使用率(%)"

    def __init__(self, raw_data: str) -> None:
        self._raw_data = raw_data

    def _extract(self) -> str:
        start_idx = self._raw_data.find(self.FORECAST_HEADER)
        raw_data = self._raw_data[start_idx:]
        end_idx = raw_data.find("\r\n\r\n")
        return raw_data[:end_idx]

    def _structuralize(self, raw_data: str) -> list[TodayForecast]:
        structured_data = []
        reader = csv.DictReader(StringIO(raw_data), delimiter=",")
        for row in reader:
            dt = datetime.strptime(f"{row['DATE']} {row['TIME']}", "%Y/%m/%d %H:%M")
            structured_data.append(
                TodayForecast(
                    dt=dt,
                    actual_result=row["当日実績(万kW)"],
                    forecast_demand=row["需要電力予測値(万kW)"],
                    forecast_supply=row["供給力予測値(万kW)"],
                ),
            )
        return structured_data

    def run(self):
        extracted_data = self._extract()
        structured_data = self._structuralize(extracted_data)
        return structured_data


class TomorrowForecastTransformer:
    DEMAND_PEAK_SUPPLY_HEADER = (
        "翌日のピーク時供給力(万kW),時間帯,供給力情報更新日,供給力情報更新時刻,ピーク時予備率(%),ピーク時使用率(%)"
    )
    DEMAND_PEAK_DEMAND_HEADER = "翌日の予想最大電力(万kW),時間帯,予想最大電力情報更新日,予想最大電力情報更新時刻"
    USAGE_PEAK_SUPPLY_HEADER = (
        "翌日の使用率ピーク時供給力(万kW),時間帯,使用率ピーク時供給力情報更新日,使用率ピーク時供給力情報更新時刻,使用率ピーク時使用率(%)"
    )
    USAGE_PEAK_DEMAND_HEADER = (
        "翌日の使用率ピーク時予想最大電力(万kW),時間帯,使用率ピーク時予想最大電力情報更新日,使用率ピーク時予想最大電力情報更新時刻"
    )
    TEMPERATURE_HEADER = "翌日の想定気温"

    def __init__(self, raw_data: str) -> None:
        self._raw_data = raw_data

    def _extract(self, header: str) -> str:
        start_idx = self._raw_data.find(header)
        raw_data = self._raw_data[start_idx:]
        end_idx = raw_data.find("\r\n\r\n")
        return raw_data[:end_idx]

    def _structuralize(self, raw_data: str, csv_mapper: dict[str, str]) -> dict:
        reader = csv.DictReader(StringIO(raw_data), delimiter=",")
        for row in reader:
            return {k: row[v] for k, v in csv_mapper.items()}
        raise Exception("data is not found")

    def run(self):
        # 需要ピーク時データ
        demand_peak_supply = self._structuralize(
            raw_data=self._extract(header=self.DEMAND_PEAK_SUPPLY_HEADER),
            csv_mapper={
                "demand_peak_time": "時間帯",
                "demand_peak_supply": "翌日のピーク時供給力(万kW)",
            },
        )
        demand_peak_demand = self._structuralize(
            raw_data=self._extract(header=self.DEMAND_PEAK_DEMAND_HEADER),
            csv_mapper={
                "demand_peak_demand": "翌日の予想最大電力(万kW)",
            },
        )
        # 使用率ピーク時データ
        usage_peak_supply = self._structuralize(
            raw_data=self._extract(header=self.USAGE_PEAK_SUPPLY_HEADER),
            csv_mapper={
                "usage_peak_time": "時間帯",
                "usage_peak_supply": "翌日の使用率ピーク時供給力(万kW)",
            },
        )
        usage_peak_demand = self._structuralize(
            raw_data=self._extract(header=self.USAGE_PEAK_DEMAND_HEADER),
            csv_mapper={
                "usage_peak_demand": "翌日の使用率ピーク時予想最大電力(万kW)",
            },
        )
        # 気温
        temperature = self._structuralize(
            raw_data=self._extract(header=self.TEMPERATURE_HEADER),
            csv_mapper={
                "temperature": "翌日の想定気温",
            },
        )
        data = {
            "date": date.today() + timedelta(days=1),
            **demand_peak_supply,
            **demand_peak_demand,
            **usage_peak_supply,
            **usage_peak_demand,
            **temperature,
        }
        return TomorrowForecast(**data)


class TokyoDataTransformer(DataTransformer):
    def run(self, raw_data: str):
        today_forecasts = TodayForecastTransformer(raw_data=raw_data).run()
        tomorrow_forecast = TomorrowForecastTransformer(raw_data=raw_data).run()
        return ForecastData(
            area=Area.tokyo,
            today_forecasts=today_forecasts,
            tomorrow_forecast=tomorrow_forecast,
        )


def collect_tokyo_forecast():
    collector = Collector(
        download_strategy=TokyoDataDownloader(),
        transformer_strategy=TokyoDataTransformer(),
    )
    return collector.run()
