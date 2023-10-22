import csv
import time
from abc import ABC, abstractmethod
from datetime import timedelta
from pathlib import Path

import boto3

from libs.constants.area import Area
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
        if tomorrow_forecast is None:
            return

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


class AthenaSaver(Saver):
    def __init__(self) -> None:
        super().__init__()
        self._database = "default"
        self._workgroup = "primary"
        session = boto3.session.Session()
        self.client = session.client("athena")

    def with_database(self, database: str):
        self._database = database
        return self

    def with_workgroup(self, workgroup: str):
        self._workgroup = workgroup
        return self

    def _execute(self, query: str):
        print(query)
        exec_id = self.client.start_query_execution(
            QueryString=query,
            WorkGroup=self._workgroup,
        )["QueryExecutionId"]
        status = self.client.get_query_execution(QueryExecutionId=exec_id)[
            "QueryExecution"
        ]["Status"]

        # ポーリング
        while status["State"] not in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            print("{}: wait query running...".format(status["State"]))
            time.sleep(5)
            status = self.client.get_query_execution(QueryExecutionId=exec_id)[
                "QueryExecution"
            ]["Status"]
        return status["State"], exec_id

    def _output_today_forecasts(self, area: Area, today_forecasts: list[TodayForecast]):
        if not today_forecasts:
            return
        target_date = today_forecasts[0].dt.date()
        target_date_p1 = target_date + timedelta(days=1)
        self._execute(
            f"""
            DELETE FROM {self._database}.today_forecast
            WHERE TIMESTAMP '{target_date.isoformat()}' < datetime
            AND datetime <= TIMESTAMP '{target_date_p1.isoformat()}'
            """
        )
        value_query = [
            f"(timestamp '{tf.dt:%Y-%m-%d %H:%M:%S}', {tf.actual_result}, {tf.forecast_demand}, {tf.forecast_supply}, '{area.value}')"
            for tf in today_forecasts
        ]
        query = f"INSERT INTO {self._database}.today_forecast VALUES {','.join(value_query)}"
        self._execute(query=query)

    def _output_tomorrow_forecast(
        self, area: Area, tomorrow_forecast: TomorrowForecast
    ):
        if tomorrow_forecast is None:
            return
        if self._database is None:
            raise Exception("need database")
        tf = tomorrow_forecast
        self._execute(
            query=f"DELETE FROM {self._database}.tomorrow_forecast WHERE date = date '{tf.date.isoformat()}'"
        )

        query = f"""
        INSERT INTO {self._database}.tomorrow_forecast
        VALUES (
            date '{tf.date.isoformat()}', '{tf.demand_peak_time}', {tf.demand_peak_supply}, {tf.demand_peak_demand},
            '{tf.usage_peak_time}', {tf.usage_peak_supply}, {tf.usage_peak_demand},
            {tf.temperature}, '{area.value}'
        )
        """
        self._execute(query=query)

    def run(self, structured_data: ForecastData):
        self._output_today_forecasts(
            area=structured_data.area, today_forecasts=structured_data.today_forecasts
        )
        self._output_tomorrow_forecast(
            area=structured_data.area,
            tomorrow_forecast=structured_data.tomorrow_forecast,
        )
