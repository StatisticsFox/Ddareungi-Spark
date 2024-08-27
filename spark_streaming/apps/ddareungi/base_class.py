from pyspark.sql import SparkSession
from redis_config import RedisConfig
from logger import Logger

class BaseClass():
    '''
    spark streaming application이 상속할 공통 클래스
    '''
    def __init__(self, app_name):
        self.app_name = app_name
        self.bootstrap_servers = "172.31.30.11:9092,172.31.39.201:9092,172.31.52.183:9092"
        self.logger = Logger(app_name)

    def main(self):
        '''
        자식 클래스 _main() 함수 실행 전 후 공통 로직 삽입시 여기에 삽입
        '''
        self._main()

    def _main(self):
        '''
        자식 클래스에서 재정의해서 사용할 것
        '''
        pass

    def create_spark_session(self, redis_master_name=None):
        if redis_master_name:
            self.redis_config = RedisConfig(redis_master_name)
            master_host, master_port, redis_auth = self.redis_config.get_redis_master()

            spark = SparkSession \
                    .builder \
                    .appName(self.app_name) \
                    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.redislabs:spark-redis_2.12:3.0.0') \
                    .config("spark.redis.host", master_host) \
                    .config("spark.redis.port", master_port) \
                    .config("spark.redis.auth", redis_auth) \
                    .getOrCreate()

            spark.sparkContext.setLogLevel("ERROR")
            return spark

        else:
            your_access_key_id, your_secret_access_key = self.get_key()
            # 아래 설정들은 $SPARK_HOME/conf/spark-defaults.conf 아래에 넣어두는게 좋습니다. -> 드라이버가 뜨는 클러스터에 적용(서버 3대에 동일하게 적용)
            # (모든 spark app에 공통으로 적용되고, access_key 같은 정보들은 서버에 있는 정보이므로 하드코딩해도 됨)
            # sparksms 옵션을 먹는 순서가 있는데 spark-defaults.conf 이 파일을 먼저 먹고 올라온다.
            # 그 다음 보는게 코드에 정의된 값을 보고 올라온다.
            # 만약 spark-defaults.conf 에도 설정이 되어있고 코드에도 설정이 되어있으면 코드에 설정된 값이 덮어씌워진다.
            # 마지막으로 spark-submit 명령어로 옵션을 주면 그 옵션이 가장 우선순위가 높다.
            spark = SparkSession \
                    .builder \
                    .appName(self.app_name) \
                    .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                    "org.apache.kafka:kafka-clients:2.7.0,"
                    "org.apache.hadoop:hadoop-aws:3.3.1,"
                    "com.amazonaws:aws-java-sdk-bundle:1.11.901") \
                    .config("spark.hadoop.fs.s3a.access.key", your_access_key_id) \
                    .config("spark.hadoop.fs.s3a.secret.key", your_secret_access_key) \
                    .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")
            return spark

    def get_key(self):
        with open("/home/ubuntu/your_access_key_id.bin", "r", encoding="UTF-8") as api_key_file:
            your_access_key_id = api_key_file.read().strip()

        with open("/home/ubuntu/your_secret_access_key.bin", "r", encoding="UTF-8") as api_key_file:
            your_secret_access_key = api_key_file.read().strip()

        return your_access_key_id, your_secret_access_key