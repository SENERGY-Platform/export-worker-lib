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

ew_lib_logger = logging.getLogger('ew-lib')
ew_lib_logger.setLevel(logging.CRITICAL)


with open("tests/resources/sources.json") as file:
    sources: list = json.load(file)

with open("tests/resources/data.json") as file:
    data: list = json.load(file)


class TestFilterConsumer(ew_lib.filter.FilterConsumer):
    def __init__(self, path, timeout=1):
        self.__timeout = timeout
        self.__queue = queue.Queue()
        with open(path, "r") as file:
            filters = json.load(file)
            for filter in filters:
                self.__queue.put(filter)

    def get_filter(self):
        try:
            return self.__queue.get(timeout=self.__timeout)
        except queue.Empty:
            pass

    def empty(self):
        return self.__queue.empty()


class TestKafkaError:
    def __init__(self, fatal=False):
        self.__fatal = fatal

    def fatal(self,):
        return self.__fatal

    def retriable(self):
        return not self.__fatal

    def str(self):
        return "error text"


class TestKafkaMessage:
    def __init__(self, value=None, err_obj=None):
        self.__value = value
        self.__err_obj = err_obj

    def error(self) -> TestKafkaError:
        return self.__err_obj

    def value(self):
        return self.__value


class TestKafkaConsumer(confluent_kafka.Consumer):
    def __init__(self):
        self.__queue = queue.Queue()
        for message in data:
            self.__queue.put(TestKafkaMessage(value=json.dumps(message)))

    def subscribe(self, topics, on_assign=None, *args, **kwargs):
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
