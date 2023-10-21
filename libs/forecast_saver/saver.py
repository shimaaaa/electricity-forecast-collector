import csv
from abc import ABC, abstractmethod
from pathlib import Path

from libs.data.forecast import ForecastData, TodayForecast, TomorrowForecast


class Saver(ABC):
    @abstractmethod
    def run(self, structured_data: ForecastData):
        pass


class CsvSaver(Saver):
    def with_output_path(self, path: str):
        self._path: Path = Path(path)
        return self

    def _output_today_forecasts(self, today_forecasts: list[TodayForecast]):
        save_data = [d.model_dump() for d in today_forecasts]
        if not save_data:
            return
        fields = list(save_data[0].keys())
        path = self._path / Path("today_forecast.csv")
        with path.open("w") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(save_data)

    def _output_tomorrow_forecast(self, tomorrow_forecast: TomorrowForecast):
        save_data = tomorrow_forecast.model_dump()
        fields = list(save_data.keys())
        path = self._path / Path("tomorrow_forecast.csv")
        with path.open("w") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerow(save_data)

    def run(self, structured_data: ForecastData):
        if self._path is None:
            raise Exception("need output path")

        self._output_today_forecasts(today_forecasts=structured_data.today_forecasts)
        self._output_tomorrow_forecast(
            tomorrow_forecast=structured_data.tomorrow_forecast
        )
