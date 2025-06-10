import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, FloatType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = "s3://mysocialdata/raw/"
output_path = "s3://mysocialdata/processed/"

sentiment_dict = {
    "love": 1.0, "awesome": 0.9, "happy": 0.8, "great": 0.85,
    "terrible": -1.0, "meh": -0.2, "okay": 0.0, "not": -0.5,
    "buggy": -0.6, "better": 0.4, "fast": 0.7, "delivery": 0.5,
    "slow": -0.7, "bad": -0.9, "hate": -1.0, "amazing": 1.0, "sushi": 0.5
}

@udf(returnType=StringType())
def clean_text_udf(text):
    if text is None:
        return ""
    text = text.lower()
    text = re.sub(r"http\S+|www\S+|@\w+|#\w+", "", text)
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

@udf(returnType=FloatType())
def sentiment_score_udf(text):
    words = text.split()
    if not words:
        return 0.0
    scores = [sentiment_dict.get(word, 0.0) for word in words]
    return float(sum(scores)) / len(words) if scores else 0.0

df = spark.read.option("multiline", "true").json(input_path)

df_cleaned = df.withColumn("normalized_text", clean_text_udf(col("text")))
df_scored = df_cleaned.withColumn("sentiment_score", sentiment_score_udf(col("normalized_text")))
final_df = df_scored.select("post_id", "user", "text", "timestamp", "normalized_text", "sentiment_score")

final_df.write.mode("overwrite").json(output_path)

job.commit()
