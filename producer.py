from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import UnknownTopicOrPartitionError
from google.protobuf.json_format import MessageToJson
import report_pb2 
import weather
import time
import grpc
import calendar


partition_count = 4

broker = 'localhost:9092'
admin = KafkaAdminClient(bootstrap_servers=[broker])

# Delete 'temperatures' topic if it exists
try:
    admin.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3)  # Deletion sometimes takes a while to reflect

try:
    admin.create_topics([NewTopic(name="temperatures", num_partitions=partition_count, replication_factor=1)])
except TopicAlreadyExistsError:
    print("already exists")

print("Topics:", admin.list_topics())


producer = KafkaProducer(
    bootstrap_servers=[broker],
    retries=10,
    acks='all',
)


for date, degrees in weather.get_next_weather(delay_sec=0.1):
    r_msg = report_pb2.Report()
    r_msg.date = date
    r_msg.degrees = degrees
    # print('r_msg format:' ,type(r_msg))
    # print('r_msg ser format:' ,type(r_msg.SerializeToString()))

    month_idx = time.strptime(date, "%Y-%m-%d").tm_mon
    month = calendar.month_name[month_idx]

    # print(f"message: date={date}, degrees={degrees}, key={month}")

    try:
        # res = producer.send("temperatures", key=str.encode(month), value=r_msg.SerializeToString(), partition=month_idx%partition_count)
        res = producer.send("temperatures", key=str.encode(month), value=r_msg.SerializeToString())
        # print(f"Message: {MessageToJson(res)}")

    except Exception as e:
        print("Failed to send message:", str(e))

    # time.sleep(0.1) 


