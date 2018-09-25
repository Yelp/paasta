# Copyright 2015-2016 Yelp Inc.
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
#
# It is imperative that this file not contain any imports from our
# dependencies. Since this file is imported from setup.py in the
# setup phase, the dependencies may not exist on disk yet.
#
# Don't bump version manually. See `make release` docs in ./Makefile
__version__ = '0.80.7'
