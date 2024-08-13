# redis_config.py
import redis
import redis.sentinel

class RedisConfig:
    def __init__(self, sentinel_hosts, master_name, redis_auth):
        self.sentinel_hosts = sentinel_hosts
        self.master_name = master_name
        self.redis_auth = redis_auth

    # def get_redis_master(self):
    #   sentinel = redis.sentinel.Sentinel(self.sentinel_hosts)
    #  master = sentinel.discover_master(self.master_name)
    #   return master


    # Redis에 데이터를 쓰는 함수
    def write_to_redis(self, batch_df, batch_id):
        batch_df.write \
        .format("org.apache.spark.sql.redis") \
        .option("table", "bike_stations") \
        .option("key.column", "stationId") \
        .mode("append") \
        .save()
