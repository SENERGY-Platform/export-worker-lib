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


class MockValidator:
    def __init__(self):
        self.called = 0

    def func(self, obj):
        assert obj
        self.called += 1
        return False


class TestFilterClient(unittest.TestCase):
    def test_get_last_update(self):
        filter_client = init_filter_client(filters=filters)
        self.assertIsNotNone(filter_client.get_last_update())

    def test_set_on_sync(self):
        event = SyncEvent()
        init_filter_client(filters=filters, sync_event=event)
        self.assertTrue(event.is_set())
        self.assertFalse(event.err)

    def test_kafka_message_error(self):
        event = SyncEvent()
        init_filter_client(filters=filters, sync_event=event, msg_errors=True)
        self.assertTrue(event.is_set())
        self.assertTrue(event.err)

    def test_validator(self):
        mock_validator = MockValidator()
        init_filter_client(filters=filters, validator=mock_validator.func)
        self.assertEqual(mock_validator.called, 13)
