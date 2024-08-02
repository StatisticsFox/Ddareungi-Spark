#!/bin/bash

# 실행 중인 producer.py 프로세스를 찾아 종료
PIDSredis=$(pgrep -f main_kafka_to_redis.py)
PIDSS3 = $(pgrep -f main_kafka_to_s3.py)
if [ -z "$PIDSS3" ]; then
    echo "No main_kafka_to_s3.py process found."
else
    for PID in $PIDSS3; do
        kill $PID
        echo "main_kafka_to_s3 with PID $PID stopped."
    done
fi

if [ -z "$PIDSredis" ]; then
    echo "No main_kafka_to_redis.py process found."
else
    for PID in $PIDSredis; do
        kill $PID
        echo "main_kafka_to_redis with PID $PID stopped."
    done
fi