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

from ._util import *
import unittest
import ew_lib
import json
import time


with open("tests/resources/filter_message_results.json") as file:
    results: list = json.load(file)


class TestFilterHandler(unittest.TestCase):
    def __test_ingestion(self, path):
        test_filter_consumer = TestFilterConsumer(path=path)
        filter_handler = ew_lib.filter.FilterHandler(filter_consumer=test_filter_consumer)
        filter_handler.start()
        while not test_filter_consumer.empty():
            time.sleep(0.1)
        for source in filter_handler.sources:
            self.assertIn(source, sources)
        self.assertIsNotNone(filter_handler.sources_timestamp)
        return filter_handler

    def __close(self, filter_handler):
        filter_handler.stop()

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
            except (ew_lib.exceptions.MessageIdentificationError, ew_lib.exceptions.NoFilterError, ew_lib.exceptions.FilterMessageError) as ex:
                self.assertIsInstance(ex, Exception)
                count += 1
        self.assertEqual(count, len(messages))
        self.__close(filter_handler=filter_handler)
