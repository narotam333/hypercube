import unittest
from unittest.mock import call, MagicMock, patch
import os
import sys

current_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(parent_dir)

from modules.load import Load
from pyspark.sql import DataFrame

project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load = Load()

class TestOpenJson(unittest.TestCase):
    def test_normal_case(self):
        config_file_path = load.open_json(f"{project_dir}/config/config.json")
        self.assertIsInstance(config_file_path, dict)
    def test_negative_case_not_exists(self):
        with self.assertRaises(SystemExit) as cm:
            load.open_json(f"{project_dir}/test/conf.json")
        self.assertEqual(cm.exception.code, 1)
    def test_negative_case_empty_file(self):
        with self.assertRaises(SystemExit) as cm:
            load.open_json(f"{project_dir}/test/config_empty.json")
        self.assertEqual(cm.exception.code, 1)

class TestReadFile(unittest.TestCase):
    def test_normal_case_csv(self):
        file_path = f"{project_dir}/../spark-data/RawData/bmrs_wind_forecast_pair.csv"
        df = load.read_file("true", "csv", file_path)
        self.assertIsInstance(df, DataFrame)
   
    def test_normal_case_json(self):
        file_path = f"{project_dir}/../spark-data/RawData/linear_orders_raw.json"
        df = load.read_file("false", "json", file_path)
        self.assertIsInstance(df, DataFrame)
 
    def test_read_file_invalid_header(self):
        file_path = f"{project_dir}/../data"
        header = "invalid"
        file_format = "csv"

        with self.assertRaises(SystemExit) as cm:
            load.read_file(header, file_format, file_path)
        self.assertEqual(cm.exception.code, 1)
    
    def test_read_file_invalid_format(self):
        file_path = f"{project_dir}/../data"
        header = "true"
        file_format = "invalid"

        with self.assertRaises(SystemExit) as cm:
            load.read_file(header, file_format, file_path)
        self.assertEqual(cm.exception.code, 1)

    def test_read_file_invalid_path(self):
        file_path = "invalid"
        header = "true"
        file_format = "csv"

        with self.assertRaises(SystemExit) as cm:
            load.read_file(header, file_format, file_path)
        self.assertEqual(cm.exception.code, 1)
if __name__ == '__main__':
    unittest.main()
