from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


log_schema = StructType([
    StructField("artist", StringType()),
    StructField("auth", StringType()),
    StructField("firstName", StringType()),
    StructField("gender", StringType()),
    StructField("itemInSession", StringType()),
    StructField("lastName", StringType()),
    StructField("length", StringType()),
    StructField("level", StringType()),
    StructField("location", StringType()),
    StructField("method", StringType()),
    StructField("page", StringType()),
    StructField("registration", StringType()),
    StructField("sessionId", StringType()),
    StructField("song", StringType()),
    StructField("status", StringType()),
    StructField("ts", StringType()),
    StructField("userAgent", StringType()),
    StructField("userId", StringType()),
    
])


