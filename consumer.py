from kafka import KafkaConsumer, TopicPartition
from report_pb2 import Report 
import threading
import json
import os
import tempfile
import calendar
import sys

def ldata(partition, partition_file):
    if not os.path.exists(partition_file):
        return {"partition": partition, "offset": 0}

    with open(partition_file, "r") as f:
        return json.load(f)


def sdata(partition_file, partition_data):
    temp_file_path = partition_file + ".tmp"
    with open(temp_file_path, "w") as temp_file:
        json.dump(partition_data, temp_file)
    os.replace(temp_file_path, partition_file)
    print("Saved file: " + partition_file)

def proc_msg(topic_partition, messages, partition_data):
    for msg in messages:
        # print("*********")
        # print(msg.partition)
        # print("*********")
        key = msg.key.decode('utf-8')
        r_msg = Report.FromString(msg.value)

        date = r_msg.date
        degrees = r_msg.degrees

        month_idx = date.split("-")[1]
        year = date.split("-")[0]
        month = calendar.month_name[int(month_idx)]

        if month not in partition_data:
            partition_data[month] = {}

        if year not in partition_data[month]:
            partition_data[month][year] = {
                "count": 1,
                "sum": degrees,
                "avg": degrees,
                "end": date,
                "start": date
            }
        else:
            if date > partition_data[month][year]["end"]:
                partition_data[month][year]["count"] += 1
                partition_data[month][year]["sum"] += degrees
                partition_data[month][year]["avg"] = partition_data[month][year]["sum"] / partition_data[month][year]["count"]
                partition_data[month][year]["end"] = date

        partition_data["offset"] = msg.offset + 1

def main(partition):
    consumer = KafkaConsumer(
    bootstrap_servers='localhost:9092',
    group_id='stats_consumer',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
)

    partition_file = "/files/partition-" + str(partition) + ".json"
    partition_data = ldata(partition, partition_file)

    consumer.assign([TopicPartition('temperatures', partition)])
    consumer.seek(TopicPartition('temperatures', partition), partition_data["offset"])

    try:
        while True:
            batch = consumer.poll(1000)
            for topic_partition, messages in batch.items():
                proc_msg(topic_partition, messages, partition_data)
                consumer.commit()
                sdata(partition_file, partition_data)

    except Exception as e:
        print("Exception here:" + str(e))
    finally:
        consumer.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("There should be atleast 2 arguments.")
        sys.exit(1)

    partitions = [int(partition) for partition in sys.argv[1:]]
    threads = []
    for part in partitions:
        thread = threading.Thread(target=main, args=[part])
        thread.start()
        threads.append(thread)
    for _ in range(len(threads)):
        threads[_].join()

