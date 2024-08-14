from redis.sentinel import Sentinel
from pyspark.sql.dataframe import DataFrame

class RedisConfig():
    def __init__(self, master_name):
        self.master_name = master_name
        if self.master_name == 'mymaster':
            self.sentinel_hosts = [
                ('172.31.16.56', 26379),
                ('172.31.46.109', 26379),
                ('172.31.48.186', 26379)
            ]
            self.redis_auth = 'mypass'
        else:
            # 향후 Sentinel 클러스터가 늘어난다면 추가할 수 있도록 작성
            pass

    def get_redis_master(self):
        sentinel = Sentinel(self.sentinel_hosts)
        host, port = sentinel.discover_master(self.master_name)
        return host, port, self.redis_auth

    def write_to_redis(self, df: DataFrame, table_nm, key_col_lst):      # 가급적 타입 지정하여 개발시 IDE의 도움을 받도록 할 것(오타 방지 등)
        '''
        가급적 공통모듈은 재활용성이 높도록 설계하는게 좋음
        전송할 데이터프레임, table_nm, key_col_lst, value_col_lst 를 입력받아
        공통 모듈로써 사용할 수 있도록 설계
        :param df: 전송할 데이터프레임
        :param table_nm: redis 테이블명
        :param key_col_lst: redis 테이블명 뒤에 붙는 key
        '''

        keys_str = ','.join(key_col_lst)
        df.write \
            .format("org.apache.spark.sql.redis") \
            .option("table", table_nm) \
            .option("key.column", keys_str) \
            .mode("append") \
            .save()
