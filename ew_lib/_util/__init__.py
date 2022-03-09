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

import logging
import typing
import hashlib
import json

logger = logging.getLogger("ew-lib")
logger.propagate = False


def hash_str(obj: str) -> str:
    return hashlib.sha256(obj.encode()).hexdigest()


def hash_list(obj: typing.List) -> str:
    return hash_str("".join(obj))


def hash_dict(obj: typing.Dict) -> str:
    items = ["{}{}".format(key, value) for key, value in obj.items()]
    items.sort()
    return hash_list(items)


def get_value(path: typing.List, obj: typing.Dict, size: int, pos: typing.Optional[int] = 0) -> typing.Any:
    if pos < size:
        return get_value(path, obj[path[pos]], size, pos + 1)
    return obj[path[pos]]


def json_to_str(obj):
    return json.dumps(obj, separators=(',', ':'))


def validate(obj, cls, name):
    assert obj, f"'{name}' can't be None"
    assert isinstance(obj, cls), f"'{name}' can't be of type '{type(obj).__name__}'"


def log_kafka_sub_action(action: str, partitions: typing.List, prefix: str):
    for partition in partitions:
        logger.info(
            f"{prefix}: subscription event: action={action} topic={partition.topic} partition={partition.partition} offset={partition.offset}"
        )
