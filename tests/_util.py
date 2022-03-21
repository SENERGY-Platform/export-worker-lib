"""
   Copyright 2022 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import ew_lib
import confluent_kafka
import logging
import json
import queue
import typing
import time
import threading

test_logger = logging.getLogger('test')
test_logger.disabled = True


with open("tests/resources/sources.json") as file:
    sources: list = json.load(file)

with open("tests/resources/data.json") as file:
    data: list = json.load(file)

with open("tests/resources/data_bad.json") as file:
    data_bad: list = json.load(file)

with open("tests/resources/filters.json") as file:
    filters: list = json.load(file)

with open("tests/resources/filters_bad.json") as file:
    filters_bad: list = json.load(file)

with open("tests/resources/results.json") as file:
    results: list = json.load(file)

with open("tests/resources/exports_results.json") as file:
    export_results: list = json.load(file)

with open("tests/resources/exports_batch_results_l2.json") as file:
    batch_results_l2: list = json.load(file)

with open("tests/resources/exports_batch_results_l3.json") as file:
    batch_results_l3: list = json.load(file)


def init_filter_client(filters, msg_errors=False, sync_event=None):
    mock_kafka_consumer = MockKafkaConsumer(data=filters, sources=False, msg_error=msg_errors)
    filter_client = ew_lib.FilterClient(
        kafka_consumer=mock_kafka_consumer,
        filter_topic="filter",
        logger=test_logger
    )
    if sync_event:
        filter_client.set_on_sync(sync_event.set, 5)
    filter_client.start()
    if sync_event:
        sync_event.wait(timeout=10)
    else:
        while not mock_kafka_consumer.empty():
            time.sleep(0.1)
    filter_client.stop()
    filter_client.join()
    return filter_client


class MockKafkaError:
    def __init__(self, fatal=False, code=None):
        self.__fatal = fatal
        self.__code = code

    def fatal(self,):
        return self.__fatal

    def retriable(self):
        return not self.__fatal

    def str(self):
        return "error text"

    def code(self):
        return self.__code


class MockKafkaMessage:
    def __init__(self, value=None, topic=None, err_obj=None, partition=0, offset=None):
        self.__value = value
        self.__err_obj = err_obj
        self.__topic = topic
        self.__partition = partition
        self.__offset = offset
        self.__timestamp = (confluent_kafka.TIMESTAMP_LOG_APPEND_TIME, time.time())

    def error(self) -> MockKafkaError:
        return self.__err_obj

    def value(self):
        return self.__value

    def topic(self):
        return self.__topic

    def timestamp(self):
        return self.__timestamp

    def partition(self):
        return self.__partition

    def offset(self):
        return self.__offset


class MockKafkaConsumer(confluent_kafka.Consumer):
    def __init__(self, data: typing.Union[typing.Dict, typing.List], sources: bool = True, msg_error: bool = False):
        self.__sources = sources
        self.__queue = queue.Queue()
        self.__offsets = dict()
        err_objs = (
            MockKafkaError(code=1),
            MockKafkaError(fatal=True, code=2),
            MockKafkaError(code=3)
        )
        if self.__sources:
            for source in data:
                offset = 0
                for message in data[source]:
                    self.__queue.put(MockKafkaMessage(value=json.dumps(message), topic=source, offset=offset))
                    offset += 1
                if msg_error:
                    for err_obj in err_objs:
                        self.__queue.put(MockKafkaMessage(err_obj=err_obj, topic=source))
        else:
            offset = 0
            for message in data:
                self.__queue.put(MockKafkaMessage(value=json.dumps(message), offset=offset))
                offset += 1
            if msg_error:
                for err_obj in err_objs:
                    self.__queue.put(MockKafkaMessage(err_obj=err_obj))

    def subscribe(self, topics, on_assign=None, *args, **kwargs):
        if self.__sources:
            for topic in topics:
                assert sources.count(topic)

    def poll(self, timeout=None):
        try:
            return self.__queue.get(timeout=timeout)
        except queue.Empty:
            pass

    def consume(self, num_messages=1, timeout=None, *args, **kwargs):
        msgs = list()
        while len(msgs) < num_messages:
            try:
                msgs.append(self.__queue.get(timeout=timeout))
            except queue.Empty:
                break
        return msgs

    def empty(self):
        return self.__queue.empty()

    def store_offsets(self, offsets):
        for tp in offsets:
            assert isinstance(tp, confluent_kafka.TopicPartition)
            key = f"{tp.topic}{tp.partition}"
            if key not in self.__offsets:
                assert tp.offset == 1
            else:
                assert tp.offset == self.__offsets[key] + 1
            self.__offsets[key] = tp.offset


class SyncEvent:
    def __init__(self):
        self.__event = threading.Event()
        self.err = None

    def set(self, err):
        self.err = err
        self.__event.set()

    def wait(self, timeout=None):
        return self.__event.wait(timeout=timeout)

    def is_set(self):
        return self.__event.is_set()

    def clear(self):
        self.__event.clear()
