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

with open("tests/resources/exports_results.json") as file:
    results: list = json.load(file)

with open("tests/resources/exports_batch_results_l2.json") as file:
    batch_results_l2: list = json.load(file)

with open("tests/resources/exports_batch_results_l3.json") as file:
    batch_results_l3: list = json.load(file)


def test_filter_ingestion(test_obj, filters):
    filter_handler = ew_lib.filter.FilterHandler()
    count = 0
    for filter in filters:
        try:
            if filter[ew_lib.clients.kafka.filter_client.Message.method] == ew_lib.clients.kafka.filter_client.Methods.put:
                filter_handler.add_filter(filter=filter[ew_lib.clients.kafka.filter_client.Message.payload])
            if filter[
                ew_lib.clients.kafka.filter_client.Message.method] == ew_lib.clients.kafka.filter_client.Methods.delete:
                filter_handler.delete_filter(
                    export_id=filter[
                        ew_lib.clients.kafka.filter_client.Message.payload][ew_lib.filter.handler.Filter.export_id]
                )
            count += 1
        except Exception:
            count += 1
    test_obj.assertEqual(count, len(filters))
    for source in filter_handler.get_sources():
        test_obj.assertIn(source, sources)
    return filter_handler


class TestKafkaError:
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


class TestKafkaMessage:
    def __init__(self, value=None, topic=None, err_obj=None, partition=0, offset=None):
        self.__value = value
        self.__err_obj = err_obj
        self.__topic = topic
        self.__partition = partition
        self.__offset = offset
        self.__timestamp = (confluent_kafka.TIMESTAMP_LOG_APPEND_TIME, time.time())

    def error(self) -> TestKafkaError:
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


class TestKafkaConsumer(confluent_kafka.Consumer):
    def __init__(self, data: typing.Dict, sources: bool = True, msg_error: bool = False):
        self.__sources = sources
        self.__queue = queue.Queue()
        self.__offsets = dict()
        err_objs = (
            TestKafkaError(code=1),
            TestKafkaError(fatal=True, code=2),
            TestKafkaError(code=3)
        )
        if self.__sources:
            for source in data:
                offset = 0
                for message in data[source]:
                    self.__queue.put(TestKafkaMessage(value=json.dumps(message), topic=source, offset=offset))
                    offset += 1
                if msg_error:
                    for err_obj in err_objs:
                        self.__queue.put(TestKafkaMessage(err_obj=err_obj, topic=source))
        else:
            offset = 0
            for message in data:
                self.__queue.put(TestKafkaMessage(value=json.dumps(message), offset=offset))
                offset += 1
            if msg_error:
                for err_obj in err_objs:
                    self.__queue.put(TestKafkaMessage(err_obj=err_obj))

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
