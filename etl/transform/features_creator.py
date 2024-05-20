import pandas as pd
from abc import ABC, abstractmethod
import db.main
from utils.s3_helper import ConnectionToS3, download_from_bucket
from etl.extraction.youbike import extract_youbike_raw_data
from etl.transform.clean_youbike_data import clean_youbike_data
from etl.transform import features_lib


class FeaturesCreator(ABC):
    @abstractmethod
    def make_prediction_features(self, station_ids: list[int]) -> pd.DataFrame:
        pass

    @abstractmethod
    def make_training_features(
        self, id: list[int], start_period: pd.Timestamp, end_period: pd.Timestamp
    ) -> pd.DataFrame:
        pass

    @property
    def model_name_version(self) -> str:
        pass


class FeaturesCreator_v1(FeaturesCreator):
    """
    Object to create features for the model.
    Transformations are made via objects declared in transformation.feature_lib.
    """

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

        # >>>>>>>>
        # TEMP HOTFIX see https://www.notion.so/justinwarambourg/Setup-new-Bike-Station-flow-4e592b533f9c48afa359c8b21fcc5228?pvs=4
        main_df = main_df[~main_df["wind_speed"].isna()].copy(deep=True)
        # TEMP HOTFIX - See https://www.notion.so/justinwarambourg/Review-conditions-for-valid-Youbike-record-7173f198f387442da15185201da6551f?pvs=4

        main_df = main_df[~main_df["pct_full"].isna()].copy(deep=True)
        # <<<<<<<<
        # Format and Validate to Schema
        main_df = features_lib.FormatToFeaturesSchema("pandas").run(main_df)
        is_schema_valid = features_lib.ValidateFeaturesSchema("pandas").run(main_df)
        if is_schema_valid == False:
            raise TypeError("Schema Validation on prediction features output failed.")

        return main_df

    def make_training_features(
        self,
        station_ids: list[int],
        start_period: pd.Timestamp,
        end_period: pd.Timestamp,
    ) -> pd.DataFrame:
        """ """
        main_df = features_lib.CreateInputTrainingFeatures("pyspark").run(
            station_ids, start_period, end_period
        )
        # # Create features
        main_df = features_lib.StationOccupancyFeatures("pyspark").run(main_df)
        main_df = features_lib.TimeFeatures("pyspark").run(main_df)
        main_df = features_lib.LagFeatures("pyspark").run(main_df)


        #TODO: Add Schema Validation
        #TODO: Add NA validation (can include in schema)
        return main_df


if __name__ == "__main__":

    test_station_ids = db.main.get_all_valid_stations_id()[:100]

    # print(
    #     FeaturesCreator_v1()
    #     .make_prediction_features(test_station_ids)
    #     .describe(include="all")
    # )

    print(
        FeaturesCreator_v1().make_training_features(
            station_ids=test_station_ids,
            start_period=pd.Timestamp("2024 04 15"),
            end_period=pd.Timestamp("2024 04 30"),
        )
    )

