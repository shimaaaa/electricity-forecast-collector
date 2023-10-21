from enum import Enum
from typing import List


class Area(str, Enum):
    hokkaido = "hokkaido"
    tohoku = "tohoku"
    tokyo = "tokyo"
    chubu = "chubu"
    hokuriku = "hokuriku"
    kansai = "kansai"
    chugoku = "chugoku"
    shikoku = "shikoku"
    kyushu = "kyushu"
    okinawa = "okinawa"

    @classmethod
    def areas(cls) -> List[str]:
        return [a.value for a in Area]
