from spark_streaming.common.base_class import BaseClass
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class DdareungiBaseClass(BaseClass):
    def __init__(self, *args):
        '''
        따릉이 데이터셋 관련된 공통 내용은 여기에 정의
        '''
        super().__init__(*args)

        self.source_topic_lst = ['bike-station-info']       # 따릉이 관련 spark application에서 공통 consume할 토픽 리스트
        self.ddareungi_schema = StructType([
            StructField("rackTotCnt", StringType()),
            StructField("stationName", StringType()),
            StructField("parkingBikeTotCnt", IntegerType()),
            StructField("shared", StringType()),
            StructField("stationLatitude", StringType()),
            StructField("stationLongitude", StringType()),
            StructField("stationId", StringType()),
            StructField("timestamp", StringType())
        ])
