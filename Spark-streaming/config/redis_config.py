import redis
import redis.sentinel

class RedisConfig:
    def __init__(self, sentinel_hosts, master_name, redis_auth):
        self.sentinel_hosts = sentinel_hosts
        self.master_name = master_name
        self.redis_auth = redis_auth

    def get_redis_master(self):
        sentinel = redis.sentinel.Sentinel(self.sentinel_hosts)
        master = sentinel.discover_master(self.master_name)
        return master
