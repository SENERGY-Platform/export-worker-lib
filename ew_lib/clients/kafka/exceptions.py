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


class KafkaMessageError(Exception):
    def __init__(self, msg, code, retry, fatal):
        self.code = code
        self.fatal = fatal
        self.retry = retry
        super().__init__(f"kafka message error: code={code} text={msg}")


class KafkaFilterClientError(Exception):
    pass


class MethodError(KafkaFilterClientError):
    def __init__(self, arg):
        super().__init__(f"unknown method: {arg}")


class SetCallbackError(KafkaFilterClientError):
    def __init__(self, arg):
        super().__init__(f"can't set callback for running client: {arg}")
