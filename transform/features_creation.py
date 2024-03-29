import pandas as pd
from abc import ABC, abstractmethod, abstractproperty
from utils.s3_helper import ConnectionToS3, download_from_bucket
from extraction.youbike import extract_youbike_raw_data
from transform.clean_youbike_data import clean_youbike_data
from . import features_lib


class FeaturesCreator(ABC):
    @abstractmethod
    def make_prediction_features(self, station_ids: list[int]) -> pd.DataFrame:
        pass

    @abstractmethod
    def make_training_features(
        self, id: list[int], start_period: pd.Timestamp, end_period: pd.Timestamp
    ) -> pd.DataFrame:
        pass

    @abstractproperty
    def model_name_version(self) -> str:
        pass


class FeaturesCreator_v1(FeaturesCreator):

    def __init__(self):
        self._model_name_version = "0.1"
        self._s3_connection = ConnectionToS3.from_env()

    @property
    def model_name_version(self):
        return self._model_name_version

    def make_prediction_features(self, station_ids: list[int]) -> pd.DataFrame:
        # Pull input data for features
        main_df = features_lib.CreateInputPredictionFeatures("pandas").run(station_ids)

        # Create features
        main_df = features_lib.StationOccupancyFeatures("pandas").run(main_df)
        main_df = features_lib.TimeFeatures("pandas").run(main_df)
        main_df = features_lib.LagFeatures("pandas").run(main_df)
        
        # Remove records for features creation
        main_df = main_df[main_df["extraction_ts"] == main_df["extraction_ts"].max()]

        # Format and Validate to Schema
        main_df = features_lib.FormatToFeaturesSchema("pandas").run(main_df)
        is_schema_valid = features_lib.ValidateFeaturesSchema("pandas").run(main_df)
        if is_schema_valid == False:
            raise TypeError("Schema Validation on prediction features output failed.")
        
        main_df.to_parquet("./tmp_data/features_debug_output.parquet")
        return main_df

    def make_training_features(
        self, id: list[int], start_period: pd.Timestamp, end_period: pd.Timestamp
    ) -> pd.DataFrame:
        pass


if __name__ == "__main__":
    test_station_ids = [
        508201032,
        508201033,
        508201034,
        508201035,
        508201036,
        508201037,
        508201038,
        508201039,
        508201040,
        508201041,
        508201013,
        508201014,
        508201015,
        508201016,
        508201017,
        508201018,
        508201019,
        508201020,
        508201021,
        508201022,
        508201023,
        508201024,
        508201025,
        508201026,
    ]
    print(
        FeaturesCreator_v1()
        .make_prediction_features(test_station_ids)
        .describe(include="all")
    )
