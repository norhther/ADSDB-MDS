from pyspark.sql import SparkSession
import os
import pydeequ
import sagemaker_pyspark
from pyspark.sql import SparkSession, Row
from pydeequ.analyzers import *
from pydeequ.profiles import *

# os.environ["SPARK_VERSION"] = r"3.3.0"
classpath = ":".join(sagemaker_pyspark.classpath_jars())  # aws-specific jars

spark = (SparkSession
         .builder
         .config("spark.driver.extraClassPath", classpath)
         .config("spark.jars.packages", pydeequ.deequ_maven_coord)
         .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
         .getOrCreate())

df = spark.read.option("header", "true").csv('landing/persistent/chocolate_part_1.csv')

analysisResult = AnalysisRunner(spark) \
    .onData(df) \
    .addAnalyzer(Size()) \
    .addAnalyzer(Completeness("rating")) \
    .run()

analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysisResult)
analysisResult_df.show()

"""
result = ColumnProfilerRunner(spark) \
    .onData(df) \
    .run()

for col, profile in result.profiles.items():
    print(profile)
"""