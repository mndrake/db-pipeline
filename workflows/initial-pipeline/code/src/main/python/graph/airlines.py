from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "airlines", label = "airlines", x = 171, y = 50, phase = 0)
@UsesDataset(id = "768", version = 0)
def airlines(spark: SparkSession) -> Source:
    fabric = Config.fabricName
    out = None

    if fabric == "dev":
        schemaArg = StructType([
            StructField("Year", IntegerType(), True),
            StructField("Month", IntegerType(), True),
            StructField("DayofMonth", IntegerType(), True),
            StructField("DayOfWeek", IntegerType(), True),
            StructField("DepTime", StringType(), True),
            StructField("CRSDepTime", IntegerType(), True),
            StructField("ArrTime", StringType(), True),
            StructField("CRSArrTime", IntegerType(), True),
            StructField("UniqueCarrier", StringType(), True),
            StructField("FlightNum", IntegerType(), True),
            StructField("TailNum", StringType(), True),
            StructField("ActualElapsedTime", StringType(), True),
            StructField("CRSElapsedTime", StringType(), True),
            StructField("AirTime", StringType(), True),
            StructField("ArrDelay", StringType(), True),
            StructField("DepDelay", StringType(), True),
            StructField("Origin", StringType(), True),
            StructField("Dest", StringType(), True),
            StructField("Distance", StringType(), True),
            StructField("TaxiIn", StringType(), True),
            StructField("TaxiOut", StringType(), True),
            StructField("Cancelled", IntegerType(), True),
            StructField("CancellationCode", StringType(), True),
            StructField("Diverted", IntegerType(), True),
            StructField("CarrierDelay", StringType(), True),
            StructField("WeatherDelay", StringType(), True),
            StructField("NASDelay", StringType(), True),
            StructField("SecurityDelay", StringType(), True),
            StructField("LateAircraftDelay", StringType(), True)
        ])
        out = spark.read\
                  .format("csv")\
                  .schema(schemaArg)\
                  .option("header", True)\
                  .option("sep", ",")\
                  .option("inferSchema", True)\
                  .load("dbfs:/databricks-datasets/asa/airlines/")
    else:
        raise ValueError("The fabric %s is not supported" % fabric)

    return out
