from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from ..config import Config
from ..prophecylibs import *

@Visual(id = "Target0", label = "Target0", x = 290, y = 50, phase = 0)
@UsesDataset(id = "769", version = 0)
def Target0(spark: SparkSession, _in: DataFrame) -> Target:
    fabric = Config.fabricName

    if fabric == "dev":
        _in.write.format("delta").mode("overwrite").saveAsTable("dave_carlson_databricks_com_db.airlines")
    else:
        raise ValueError("The fabric %s is not supported" % fabric)

    return None
