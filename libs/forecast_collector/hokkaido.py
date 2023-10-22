import csv
from datetime import date, datetime, timedelta
from io import StringIO

from libs.constants.area import Area
from libs.data.forecast import ForecastData, TodayForecast, TomorrowForecast
from libs.forecast_collector.base import Collector, DataDownloader, DataTransformer


class HokkaidoDataDownloader(DataDownloader):
    URL = "https://denkiyoho.hepco.co.jp/area/data/juyo_01_{target_date}.csv"

    def run(self) -> str:
        target_date = date.today()
        url = self.URL.format(target_date=target_date.strftime("%Y%m%d"))
        return self._fetch(url=url)


class TodayForecastTransformer:
    FORECAST_HEADER = "DATE,TIME,当日実績(万kW),予測値(万kW),使用率(%),供給力想定値(万kW)"

    def __init__(self, raw_data: str) -> None:
        self._raw_data = raw_data

    def _extract(self) -> str:
        start_idx = self._raw_data.find(self.FORECAST_HEADER)
        raw_data = self._raw_data[start_idx:]
        end_idx = raw_data.find("\n\n")
        return raw_data[:end_idx]

    def _structuralize(self, raw_data: str) -> list[TodayForecast]:
        structured_data = []
        reader = csv.DictReader(StringIO(raw_data), delimiter=",")
        for row in reader:
            dt = datetime.strptime(f"{row['DATE']} {row['TIME']}", "%Y/%m/%d %H:%M")
            actual_result = row["当日実績(万kW)"]
            if not actual_result:
                actual_result = 0
            structured_data.append(
                TodayForecast(
                    dt=dt,
                    actual_result=actual_result,
                    forecast_demand=row["予測値(万kW)"],
                    forecast_supply=row["供給力想定値(万kW)"],
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
        raise ValueError("data is not found")

    def run(self):
        try:
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
        except ValueError:
            # 18時以降じゃないとデータが出てこない
            return None


class HokkaidoDataTransformer(DataTransformer):
    def run(self, raw_data: str):
        today_forecasts = TodayForecastTransformer(raw_data=raw_data).run()
        tomorrow_forecast = TomorrowForecastTransformer(raw_data=raw_data).run()
        return ForecastData(
            area=Area.hokkaido,
            today_forecasts=today_forecasts,
            tomorrow_forecast=tomorrow_forecast,
        )


def collect_hokkaido_forecast():
    collector = Collector(
        download_strategy=HokkaidoDataDownloader(),
        transformer_strategy=HokkaidoDataTransformer(),
    )
    return collector.run()
