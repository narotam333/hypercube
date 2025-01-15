import psycopg2
import json
import sys
import os
import pyspark
from pyspark.sql import SparkSession, functions as F

project_dir = os.path.dirname(os.path.abspath(__file__))

# Postgres config file
pg_config_file = f"{project_dir}/../config/config.json"

class Load:
   
    def __init__(self):
        self.spark = SparkSession \
                        .builder.master("local[1]") \
                        .appName('HyperCube') \
                        .config("spark.security.manager.enabled", "false") \
                        .getOrCreate()
        
        conf = self.open_json(pg_config_file)
        self.host = conf.get('pg', {}).get('host')
        self.port = conf.get('pg', {}).get('port')
        self.database = conf.get('pg', {}).get('database')
        self.user = conf.get('pg', {}).get('user')
        self.password = conf.get('pg', {}).get('password')
        self.driver = "org.postgresql.Driver"
        
    def open_json(self, filepath):
        # open a json file and return a dict
        if os.path.exists(filepath) and os.path.getsize(filepath) != 0:
            with open(filepath, "r") as f:
                data = json.load(f)
            return data
        elif not os.path.exists(filepath):
            print("ERROR: "+filepath+" doesn't exists")
            sys.exit(1)
        elif os.path.getsize(filepath) == 0:
            print("ERROR: "+filepath+" is empty")
            sys.exit(1)

    def read_file(self, header, file_format, file_path):
        if file_format not in ("csv", "json"):
            print("Not a valid file format")
            sys.exit(1)
        elif not(os.path.exists(file_path)):
            print("File path doesn't exists")
            sys.exit(1)
        else:
            df = self.spark \
                     .read \
                     .format(file_format) \
                     .option("header", header) \
                     .option("multiline", "true") \
                     .load(file_path)

            return df

    # Create postgres tables
    def create_pg_tables(self, tbl_list):

        # open postgres conn
        conn = psycopg2.connect(host=self.host, port=self.port, database=self.database, user=self.user, password=self.password)
        cursor = conn.cursor()

        for table in tbl_list:
            cursor.execute(table)
            conn.commit()

        # Close the connection
        cursor.close()
        conn.close()

    def postgres_load(self, df, table_name):
        jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        connection_properties = {
            "user": self.user,
            "password": self.password,
            "driver": self.driver
        }

        df.write.jdbc(jdbc_url, table_name, mode="overwrite", properties=connection_properties)

