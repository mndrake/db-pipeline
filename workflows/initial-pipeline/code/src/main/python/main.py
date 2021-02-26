from pyspark.sql import SparkSession


class Main:

    def graph(self, spark: SparkSession):
        df_Sourceairlines: Source = Sourceairlines(spark)
        Targetairlines(spark, df_Sourceairlines)

    def main(self):
        spark = SparkSession.builder.appName("initialpipeline").getOrCreate()
        spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
        self.graph(spark)

if __name__ == __main__:
    Main().main()
