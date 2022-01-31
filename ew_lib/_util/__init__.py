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

logger = logging.getLogger('ew-lib')
logger.propagate = False


def hash_dict(obj: typing.Dict):
    items = ["{}{}".format(key, value) for key, value in obj.items()]
    items.sort()
    items_str = "".join(items)
    return hashlib.sha256(items_str.encode()).hexdigest()


def get_value(path: typing.List, obj: typing.Dict, size: int, pos: typing.Optional[int] = 0) -> typing.Any:
    if pos < size:
        return get_value(path, obj[path[pos]], size, pos + 1)
    return obj[path[pos]]
