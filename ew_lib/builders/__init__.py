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


import typing


def dict_builder(mapper: typing.Generator) -> typing.Dict[str, typing.Any]:
    data = dict()
    for key, value in mapper:
        data[key] = value
    return data


def string_list_builder(mapper: typing.Generator) -> typing.List[str]:
    data = list()
    for key, value in mapper:
        data.append(f'{key}={value}')
    return data


def tuple_list_builder(mapper: typing.Generator) -> typing.List[typing.Tuple[str, typing.Any]]:
    data = list()
    for key, value in mapper:
        data.append((key, value))
    return data
