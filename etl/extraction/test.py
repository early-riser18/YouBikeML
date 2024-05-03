from etl.extraction import youbike
import unittest
from unittest.mock import Mock, patch
from utils.s3_helper import ConnectionToS3, download_from_bucket
import pandas as pd


class TestExtraction(unittest.TestCase):

    @patch("requests.request")
    def test_get_youbike_data_success(self, mock_request):
        download_from_bucket(
            s3_bucket=ConnectionToS3.from_env(),
            filter=f"test/sample_youbike_valid_raw_text_response.csv",
            dest_dir="./tmp/test",
        )
        with open(f"./tmp/test/sample_youbike_valid_raw_text_response.csv") as f:
            res_text = f.read()
        mock_request.return_value = Mock(status_code=200, text=res_text)

        result = youbike.get_youbike_data()
        self.assertIsInstance(result, youbike.YoubikeSnapshot)
        self.assertIsInstance(result.body, pd.DataFrame)

    @patch("requests.request")
    def test_get_youbike_data_failure(self, mock_request):
        mock_request.return_value = Mock(status_code=400)

        with self.assertRaises(Exception) as context:
            youbike.get_youbike_data()

        # General
        mock_request.assert_called_with(
            "GET",
            "https://gcs-youbike2-linebot.microprogram.tw/latest-data/youbike-station.csv",
        )


unittest.main(verbosity=2)
