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


import ew_lib.filter._handler
import logging
import json
import queue

ew_lib_logger = logging.getLogger('ew-lib')
ew_lib_logger.setLevel(logging.CRITICAL)


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


with open("tests/resources/sources.json") as file:
    sources = json.load(file)

with open("tests/resources/data.json") as file:
    messages = json.load(file)

with open("tests/resources/filter_message_results.json") as file:
    results = json.load(file)