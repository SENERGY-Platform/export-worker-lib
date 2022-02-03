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

import unittest
import ew_lib.filter._handler
import logging
import json
import queue
import time

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


class TestFilterHandler(unittest.TestCase):
    def __test_ingestion(self, path):
        test_filter_consumer = TestFilterConsumer(path=path)
        filter_handler = ew_lib.filter._handler.FilterHandler(filter_consumer=test_filter_consumer)
        filter_handler.start()
        while not test_filter_consumer.empty():
            time.sleep(0.1)
        for source in filter_handler.sources:
            self.assertIn(source, sources)
        self.assertIsNotNone(filter_handler.sources_timestamp)
        return filter_handler

    def __close(self, filter_handler):
        filter_handler.stop()
        filter_handler.join()

    def test_ingestion_good_filters(self):
        filter_handler = self.__test_ingestion(path="tests/resources/filters.json")
        self.__close(filter_handler=filter_handler)

    def test_ingestion_erroneous_filters(self):
        filter_handler = self.__test_ingestion(path="tests/resources/filters_bad.json")
        self.__close(filter_handler=filter_handler)

    def test_filter_message_good_filters(self):
        filter_handler = self.__test_ingestion(path="tests/resources/filters.json")
        count = 0
        for message in messages:
            try:
                result = json.loads(json.dumps(filter_handler.filter_message(msg=message)))
                self.assertIn(result, results)
                count += 1
            except (ew_lib.exceptions.MessageIdentificationError, ew_lib.exceptions.NoFilterError) as ex:
                self.assertIsInstance(ex, Exception)
                count += 1
        self.assertEqual(count, len(messages))
        self.__close(filter_handler=filter_handler)

    def test_filter_message_erroneous_filters(self):
        filter_handler = self.__test_ingestion(path="tests/resources/filters_bad.json")
        count = 0
        for message in messages:
            try:
                filter_handler.filter_message(msg=message)
                count += 1
            except (ew_lib.exceptions.MessageIdentificationError, ew_lib.exceptions.NoFilterError, ew_lib.exceptions.FilterMessageError) as ex:
                self.assertIsInstance(ex, Exception)
                count += 1
        self.assertEqual(count, len(messages))
        self.__close(filter_handler=filter_handler)


if __name__ == '__main__':
    unittest.main()

# python -m unittest tests.test_filter_handler -v
