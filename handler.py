import logging

from libs.constants.area import Area
from libs.forecast_collector import collect_hokkaido_forecast, collect_tokyo_forecast
from libs.forecast_saver import AthenaSaver

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

AREA_COLLECTOR_MAPPING = {
    Area.hokkaido: collect_hokkaido_forecast,
    Area.tokyo: collect_tokyo_forecast,
}


def run(event, context):
    # saver = CsvSaver().with_output_path("./")
    saver = AthenaSaver()
    # collect electricity forecast data
    for area in Area:
        collector = AREA_COLLECTOR_MAPPING.get(area)
        if collector is None:
            logger.warning(f"{area.name} importer is not defined.")
            continue
        collected_data = collector()
        saver.run(structured_data=collected_data)


run(None, None)
