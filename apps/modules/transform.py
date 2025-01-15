import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.window import Window

class Transform:

    def __init__(self):
        self.spark = SparkSession.getActiveSession()

    # Dedupe data
    def dedupe(self, df):
      df = df.dropDuplicates()
      return df

    # Valid data
    def impute_data(self, df):
        
        # remove string characters from the numeric field
        df = df.withColumn('initialForecastSpnGeneration', F.regexp_replace(F.col('initialForecastSpnGeneration'), '[^0-9]', ''))
        df = df.withColumn('latestForecastSpnGeneration', F.regexp_replace(F.col('latestForecastSpnGeneration'), '[^0-9]', ''))
    
        # convert string columns to required data types
        df = df.withColumn('outTurnPublishingPeriodCommencingTime', F.to_timestamp(F.col('outTurnPublishingPeriodCommencingTime'), 'dd/MM/yyyy HH:mm')) \
               .withColumn('initialForecastSpnGeneration', F.col('initialForecastSpnGeneration').cast(FloatType())) \
               .withColumn('latestForecastSpnGeneration', F.col('latestForecastSpnGeneration').cast(FloatType())) \
               .withColumn('fuelTypeGeneration', F.col('fuelTypeGeneration').cast(IntegerType()))
        
        # calculating missing EFA values based on outTurnPublishingPeriodCommencingTime
        df = df.withColumn('EFA', \
                  F.when( \
                     (F.hour(F.col('outTurnPublishingPeriodCommencingTime')) >= 23) |  
                     (F.hour(F.col('outTurnPublishingPeriodCommencingTime')) < 3), 1 
                  ).when( \
                     (F.hour(F.col('outTurnPublishingPeriodCommencingTime')) >= 3) & 
                     (F.hour(F.col('outTurnPublishingPeriodCommencingTime')) < 7), 2 
                  ).when( \
                     (F.hour(F.col('outTurnPublishingPeriodCommencingTime')) >= 7) & 
                     (F.hour(F.col('outTurnPublishingPeriodCommencingTime')) < 11), 3 
                  ).when( \
                     (F.hour(F.col('outTurnPublishingPeriodCommencingTime')) >= 11) & 
                     (F.hour(F.col('outTurnPublishingPeriodCommencingTime')) < 15), 4 
                  ).when( \
                     (F.hour(F.col('outTurnPublishingPeriodCommencingTime')) >= 15) & 
                     (F.hour(F.col('outTurnPublishingPeriodCommencingTime')) < 19), 5 
                  ).when( \
                     (F.hour(F.col('outTurnPublishingPeriodCommencingTime')) >= 19) &
                     (F.hour(F.col('outTurnPublishingPeriodCommencingTime')) < 23), 6 
                  ).otherwise(None).cast(IntegerType()) \
             )
        
        # converting to pandas df to impute missing spn generation values
        df_pandas = df.toPandas()
        
        # interpolating values given the data is time series based
        df_pandas['initialForecastSpnGeneration'] = df_pandas['initialForecastSpnGeneration'].interpolate(method='linear')
        df_pandas['latestForecastSpnGeneration'] = df_pandas['latestForecastSpnGeneration'].interpolate(method='linear')
        df_pandas['fuelTypeGeneration'] = df_pandas['fuelTypeGeneration'].interpolate(method='linear')
        
        # converting pandas df back to spark df
        df = self.spark.createDataFrame(df_pandas)

        return df

    # Invalid data
    # Filter records where outTurnPublishingPeriodCommencingTime is missing
    def filter_data(self, df):
        df = df.filter("outTurnPublishingPeriodCommencingTime is not null or outTurnPublishingPeriodCommencingTime != ''")
        return df
    
    # Join wind forecast and orders data
    def join_data(self, df1, df2):
        df2 = df2.drop("EFA")
        joined_df = df1.join(df2, df1.outTurnPublishingPeriodCommencingTime == df2.DeliveryStart, "inner")
        return joined_df

    ### feature engineering transformations ###
    # Rolling medians over a 6-hour window 
    def rolling_data(self, df):
       
        df = df.select("outTurnPublishingPeriodCommencingTime", "initialForecastSpnGeneration", "ExecutedVolume") 
        df = df.withColumn('ExecutedVolume', F.col('ExecutedVolume').cast(IntegerType())) 
        
        df = df.groupBy("outTurnPublishingPeriodCommencingTime").agg(F.sum("initialForecastSpnGeneration").alias("initialForecastSpnGeneration"), \
                                                                     F.sum("ExecutedVolume").alias("ExecutedVolume"))     

        df_pandas = df.toPandas()
        df_pandas = df_pandas.sort_values('outTurnPublishingPeriodCommencingTime')
        df_pandas.set_index('outTurnPublishingPeriodCommencingTime', inplace=True)
        
        df_pandas['rolling_median_initialForecastSpnGeneration'] = df_pandas['initialForecastSpnGeneration'].rolling('6H').median()
        df_pandas['rolling_median_ExecutedVolume'] = df_pandas['ExecutedVolume'].rolling('6H').median()

        df_pandas.reset_index(inplace=True)

        # converting pandas df back to spark df
        df = self.spark.createDataFrame(df_pandas)

        return df

    # daily and weekly aggregate view of the data
    def daily_agg(self, df):
        df = df.withColumn("day", F.date_format("DeliveryStart", "yyyy-MM-dd"))
       
        df = df.groupBy("day").agg( \
                         F.sum("initialForecastSpnGeneration").alias("sum_initialForecastSpnGeneration"),\
                         F.sum("ExecutedVolume").alias("sum_ExecutedVolume"),\
                        )
        return df

    def weekly_agg(self, df):     
        df = df.withColumn("week", F.weekofyear("DeliveryStart"))
        
        df = df.groupBy("week").agg( \
                          F.sum("initialForecastSpnGeneration").alias("sum_initialForecastSpnGeneration"),\
                          F.sum("ExecutedVolume").alias("sum_ExecutedVolume"),\
                         )
        return df

