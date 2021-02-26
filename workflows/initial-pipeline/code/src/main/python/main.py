from pyspark.sql import SparkSession


class Main:

    def graph(self, spark: SparkSession):
        df_airlines: Source = airlines(spark)
        Target0(spark, df_airlines)

    def main(self):
        spark = SparkSession.builder.appName("initialpipeline").getOrCreate()
        spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
        self.graph(spark)

if __name__ == __main__:
    Main().main()
