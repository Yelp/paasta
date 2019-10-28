# Copyright 2019 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import TypeVar

from typing_extensions import Protocol


T = TypeVar('T')


class XValueDiff(Protocol[T]):
    def __mul__(self, other: int) -> 'XValueDiff[T]': ...

    def __truediv__(self, other: 'XValueDiff[T]') -> float: ...


class XValue(Protocol[T]):
    def __add__(self, other: XValueDiff[T]) -> 'XValue[T]': ...

    def __sub__(self, other: 'XValue[T]') -> XValueDiff[T]: ...

    def __floordiv__(self, other: 'XValue[T]') -> float: ...

    def __lt__(self, other: 'XValue[T]') -> bool: ...

    def __ge__(self, other: 'XValue[T]') -> bool: ...

    def __mod__(self, other: 'XValue[T]') -> int: ...
