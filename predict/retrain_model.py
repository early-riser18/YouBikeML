import pandas as pd
from db.main import get_all_valid_stations_id
from etl.transform.spark_app import SparkApp
from etl.transform import features_creator
from etl.transform import features_creator

spark_session = SparkApp.get_instance(log_level="DEBUG")
test_station_ids = get_all_valid_stations_id()

training_features = features_creator.FeaturesCreator_v1().make_training_features(
    test_station_ids, pd.Timestamp("2024 04 15"), pd.Timestamp("2024 04 30")
)
# Train Model
# Upload Model

print(training_features.sample(50 / training_features.count()))
print(training_features.sample(50 / training_features.count()).show())