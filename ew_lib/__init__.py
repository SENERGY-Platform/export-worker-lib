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

__version__ = '0.29.5'
__title__ = 'export-worker-lib'
__description__ = 'Library for implementing export-workers to transfer data from the SENERGY streaming platform to data sinks.'
__url__ = 'https://github.com/SENERGY-Platform/export-worker-lib'
__author__ = 'Yann Dumont'
__license__ = 'Apache License 2.0'
__copyright__ = 'Copyright 2022 InfAI (CC SES)'


from .clients import *
import ew_lib.exceptions
