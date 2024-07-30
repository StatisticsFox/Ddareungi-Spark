from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def get_schema():
    return StructType([
        StructField("rackTotCnt", StringType()),
        StructField("stationName", StringType()),
        StructField("parkingBikeTotCnt", IntegerType()),
        StructField("shared", StringType()),
        StructField("stationLatitude", StringType()),
        StructField("stationLongitude", StringType()),
        StructField("stationId", StringType()),
        StructField("timestamp", StringType())
    ])
