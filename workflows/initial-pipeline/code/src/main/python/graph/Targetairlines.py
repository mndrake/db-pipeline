from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Targetairlines", label = "Target-airlines", x = 290, y = 50, phase = 0)
@UsesDataset(id = "769", version = 0)
def Targetairlines(spark: SparkSession, _in: DataFrame) -> Target:
    fabric = Config.fabricName

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
        _in.write\
            .format("delta")\
            .option("optimizeWrite", True)\
            .mode("overwrite")\
            .partitionBy("Year", "Origin")\
            .save("dbfs:/home/dave.carlson@databricks.com/datasets/airlines")
    else:
        raise ValueError("The fabric %s is not supported" % fabric)

    return None
