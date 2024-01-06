from kafka import KafkaConsumer
from report_pb2 import Report 
import json

consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    group_id='debug',
    auto_offset_reset='latest',
    enable_auto_commit=False
)
consumer.subscribe(['temperatures'])
# print(consumer.assignment())
# print("I am here.")
# batch = consumer.poll(10)
# print(batch.items())


while True:
    # print("Hi")
    batch = consumer.poll(1000)
    # print(consumer.assignment())
    for topic_partition, messages in batch.items():
        # print("hello")
        for msg in messages:
            try:
                r_msg = Report.FromString(msg.value)
                print({
                    'partition': msg.partition,
                    'key': msg.key.decode('utf-8'),
                    'date': r_msg.date,
                    'degrees': r_msg.degrees
                })
            except Exception as e:
                print(f"Failed to deserialize message: {e}")


