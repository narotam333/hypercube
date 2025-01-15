#!/usr/bin/python3

import sys
import os

# Importing required classes from modules folder
from modules.load import Load
from modules.transform import Transform

project_dir = os.path.dirname(os.path.abspath(__file__))

# Postgres tables
wind_forecast = "CREATE TABLE IF NOT EXISTS wind_forecast (Column1 TEXT, \
    recordType TEXT, \
    startTimeOfHalfHrPeriod TIMESTAMP,\
    settlementPeriod INT,\
    initialForecastPublishingPeriodCommencingTime TIMESTAMP,\
    initialForecastSpnGeneration FLOAT,\
    latestForecastPublishingPeriodCommencingTime TIMESTAMP,\
    latestForecastSpnGeneration FLOAT,\
    outTurnPublishingPeriodCommencingTime TIMESTAMP,\
    fuelTypeGeneration FLOAT,\
    activeFlag BOOLEAN,\
    EFA INT);"
linear_orders = """CREATE TABLE IF NOT EXISTS linear_orders (OrderEntryTime TIMESTAMP,\
    "1V" NUMERIC,\
    "3P" NUMERIC,\
    "3V" NUMERIC,\
    DeliveryEnd TIMESTAMP,\
    OrderEntryUser TEXT,\
    OrderPeriodID TEXT,\
    BiddingLevelName TEXT,\
    OrderID TEXT,\
    "2V" NUMERIC,\
    "2P" NUMERIC,\
    ExecutedVolume NUMERIC,\
    "4V" NUMERIC,\
    MemberName TEXT,\
    "4P" NUMERIC,\
    DeliveryStart TIMESTAMP,\
    SettlementCurrency TEXT,\
    MarketName TEXT,\
    EFA NUMERIC,\
    TradeID TEXT,\
    _full_text TEXT,\
    Portfolio TEXT,\
    _id BIGINT,\
    "1P" NUMERIC);"""

delivered_orders = """CREATE TABLE IF NOT EXISTS delivered_orders (Column1 TEXT, \
    recordType TEXT, \
    startTimeOfHalfHrPeriod TIMESTAMP,\
    settlementPeriod INT,\
    initialForecastPublishingPeriodCommencingTime TIMESTAMP,\
    initialForecastSpnGeneration FLOAT,\
    latestForecastPublishingPeriodCommencingTime TIMESTAMP,\
    latestForecastSpnGeneration FLOAT,\
    outTurnPublishingPeriodCommencingTime TIMESTAMP,\
    fuelTypeGeneration FLOAT,\
    activeFlag BOOLEAN,\
    EFA INT,\
    OrderEntryTime TIMESTAMP,\
    "1V" NUMERIC,\
    "3P" NUMERIC,\
    "3V" NUMERIC,\
    DeliveryEnd TIMESTAMP,\
    OrderEntryUser TEXT,\
    OrderPeriodID TEXT,\
    BiddingLevelName TEXT,\
    OrderID TEXT,\
    "2V" NUMERIC,\
    "2P" NUMERIC,\
    ExecutedVolume NUMERIC,\
    "4V" NUMERIC,\
    MemberName TEXT,\
    "4P" NUMERIC,\
    DeliveryStart TIMESTAMP,\
    SettlementCurrency TEXT,\
    MarketName TEXT,\
    TradeID TEXT,\
    _full_text TEXT,\
    Portfolio TEXT,\
    _id BIGINT,\
    "1P" NUMERIC);"""

rolling_median = """CREATE TABLE IF NOT EXISTS rolling_median (DeliveryStart TIMESTAMP,\
    rolling_median_initialForecastSpnGeneration NUMERIC,\
    rolling_median_ExecutedVolume NUMERIC);"""

daily_agg = """CREATE TABLE IF NOT EXISTS daily_agg (day DATE,\
    sum_initialForecastSpnGeneration NUMERIC,\
    sum_ExecutedVolume NUMERIC\
    );"""

weekly_agg = """CREATE TABLE IF NOT EXISTS weekly_agg (week NUMERIC,\
    sum_initialForecastSpnGeneration NUMERIC,\
    sum_ExecutedVolume NUMERIC\
    );"""

# Main function
def main():

    load = Load()
    transform = Transform()
   
    ### Create postgres tables ###
    load.create_pg_tables([wind_forecast, linear_orders, delivered_orders, rolling_median, daily_agg, weekly_agg])

    ### Reading raw data ###
    header = "true"
    file_format = "csv"
    print(f"Reading wind forecast data...")
    src_wind_forecast_df = load.read_file(header, file_format, "/opt/spark-data/RawData/bmrs_wind_forecast_pair.csv")
    #src_wind_forecast_df.show()

    header = "false"
    file_format = "json"
    print(f"Reading orders data...")
    src_order_df = load.read_file(header, file_format, "/opt/spark-data/RawData/linear_orders_raw.json")
    
    order_df = src_order_df.selectExpr("explode(result.records) as record").select("record.*")
    #order_df.show()

    # Remove duplicate orders
    dedupe_order_df = transform.dedupe(order_df)

    # Filter Invalid data
    filtered_wind_forecast_df = transform.filter_data(src_wind_forecast_df)
    
    # Remove duplicate data
    dedupe_wind_forecast_df = transform.dedupe(filtered_wind_forecast_df)
    
    # Impute missing data
    imputed_wind_forecast_df = transform.impute_data(dedupe_wind_forecast_df)
    #imputed_wind_forecast_df.show(1500)

    ### Write to DB table ###
    # Loading wind forecast data
    table_name = "wind_forecast"
    load.postgres_load(imputed_wind_forecast_df, table_name)

    # Loading orders data
    table_name = "linear_orders"
    load.postgres_load(dedupe_order_df, table_name)
 
    ### Join wind forecast and orders data ###
    delivered_orders_df = transform.join_data(imputed_wind_forecast_df, order_df)
    delivered_orders_df.show()

    ### Write to DB table ###
    # Loading delivered orders
    table_name = "delivered_orders"
    load.postgres_load(delivered_orders_df, table_name)

    ### Feature engineering forecast model calculations ###
    # rolling 6 hours window data for 'initialForecastSpnGeneration' and 'ExecutedVolume'
    rolling_df = transform.rolling_data(delivered_orders_df)

    ### Write to DB table ###
    # Loading rolling median data
    table_name = "rolling_median"
    load.postgres_load(rolling_df, table_name)

    # daily agg. of data
    daily_agg_df = transform.daily_agg(delivered_orders_df)

    ### Write to DB table ###
    # Loading daily agg data
    table_name = "daily_agg"
    load.postgres_load(daily_agg_df, table_name)

    # weekly agg. of data
    weekly_agg_df = transform.weekly_agg(delivered_orders_df) 
  
    ### Write to DB table ###
    # Loading weekly agg data
    table_name = "weekly_agg"
    load.postgres_load(weekly_agg_df, table_name)

if __name__ == "__main__":
    main()
