from abc import ABC, abstractmethod
import pandas as pd
from utils.s3_helper import ConnectionToS3
from etl.transform.features_creator import FeaturesCreator, FeaturesCreator_v1
from io import BytesIO
import pickle
from sklearn.linear_model import LinearRegression


class ForecastModel(ABC):

    def __init__(self, model_name: str, features_creator: FeaturesCreator) -> None:
        self.model_path = "model/" + model_name + ".pkl"
        self.features_creator = features_creator
        self.s3_connection = ConnectionToS3.from_env()
        self.model = self.__get_model()

    @abstractmethod
    def forecast(station_ids: list[int]) -> list:
        """
        Ask feature creator for features per station id, make a forecast and create StationForecast Object (format TBD)
        """
        pass

    def __get_model(self) -> LinearRegression:
        """retrieve pickled model from s3 and deserialize it into a Python Object"""
        bucket = self.s3_connection.resource.Bucket(self.s3_connection.bucket_name)
        try:
            s3_res = bucket.Object(self.model_path).get()
            buffer = BytesIO(s3_res["Body"].read())
        # Need to refactor to handle correct exception when not found.
        except Exception as e:
            Exception("Encountered exception: ", e)
            ## Once you handle the exception, set a Warning instead and proceed by returning None. It means the object expects to be trained.

        buffer.seek(0)
        model = pickle.load(buffer)
        return model


class RegressionYouBikeModel(ForecastModel):
    """
    Object to make forecast about Youbikes using a Regression approach and train a new model.
    Features required for the model are declared in feature_creator

    """

    def __init__(self, model_name: str, features_creator: FeaturesCreator):
        super().__init__(model_name, features_creator)
        # MODEL_PATH = "model/model_2024-03-29.pkl"

        # COULD ADD A "NBR OF FUTURE PREDICTED TS as options. For now default is 1 - later should be 3H so 6"

    def __make_forecast(self, features: pd.DataFrame) -> pd.DataFrame:
        """
        Wrapper function to the underlying prediction model's predict function.
        Adds custom logic.
        """

        prediction = self.model.predict(features)
        features.reset_index(inplace=True)

        ## Ideally the following should be abstracted away, as it deals with a concrete implementation of the features input/
        # Ideally, the exact features schema in and prediction out and their transformations are logically grouped together and only plugged in in an abstracted way
        forecast_df = features[["station_id", "pct_full", "extraction_ts"]].copy(
            deep=True
        )

        # Custom Logic
        forecast_df["pct_full_pred_rel_change"] = prediction
        forecast_df["30m_fwd_pct_full"] = (
            forecast_df["pct_full"] + 1 + forecast_df["pct_full_pred_rel_change"]
        )

        # Clip to possible range
        forecast_df["30m_fwd_pct_full"] = forecast_df["30m_fwd_pct_full"].clip(
            lower=0, upper=1
        )

        forecast_df["forecast_ts"] = (
            forecast_df["extraction_ts"] + pd.Timedelta(minutes=30)
        ).dt.round("min")

        return forecast_df[["station_id", "30m_fwd_pct_full", "forecast_ts"]]

    def forecast(self, station_ids: list[int]) -> pd.DataFrame:
        print("forecasting for ids: ", station_ids)
        features_df = self.features_creator.make_prediction_features(station_ids)
        forecast_df = self.__make_forecast(features_df)

        t_0_records = features_df[["station_id", "pct_full"]].rename(
            columns={"pct_full": "fill_rate"}
        )
        t_0_records["relative_ts"] = 0

        t_1_records = forecast_df.rename(
            columns={
                "30m_fwd_pct_full": "fill_rate",
            }
        )
        t_1_records["relative_ts"] = 1

        results = pd.concat([t_0_records, t_1_records]).reset_index(drop=True)
        results = pd.merge(
            results,
            features_df[["station_id", "extraction_ts"]],
            on="station_id",
            how="left",
        )
        results["run_ts"] = pd.Timestamp.now()
        results = results.rename(columns={"extraction_ts": "base_ts"})

        return results


if __name__ == "__main__":

    test_station_ids = [
        508201032,
        501208101,
        501216049,
        501210126,
        501209089,
        508201041,
        508201014,
        500610054,
        500610055,
        500610056,
        500311022,
        500310006,
        508201034,
        508201036,
        508201038,
        508201039,
        508201041,
        508201013,
        508201014,
        508201016,
        508201017,
        508201019,
        508201020,
        508201021,
        508201023,
        508201024,
        508201026,
    ]

    print(
        RegressionYouBikeModel(
            model_name="model_2024-03-29", features_creator=FeaturesCreator_v1()
        ).forecast(test_station_ids)
    )
    # print(RegressionYouBikeModel().model)
