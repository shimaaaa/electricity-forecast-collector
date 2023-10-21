from abc import ABC, abstractmethod
from typing import List

import requests

from libs.data.forecast import ForecastData


class DataDownloader(ABC):
    def _fetch(self, url: str):
        header = {"User-Agent": ""}
        response = requests.get(url, headers=header)
        response.encoding = response.apparent_encoding
        response.raise_for_status()
        return response.text

    @abstractmethod
    def run() -> str:
        pass


class DataTransformer(ABC):
    @abstractmethod
    def run(cls, raw_data: str) -> List[ForecastData]:
        pass


class Collector:
    def __init__(
        self,
        download_strategy: DataDownloader,
        transformer_strategy: DataTransformer,
    ) -> None:
        self._download_strategy = download_strategy
        self._transformer_strategy = transformer_strategy

    def run(self) -> List[ForecastData]:
        raw_data = self._download_strategy.run()
        return self._transformer_strategy.run(raw_data=raw_data)
