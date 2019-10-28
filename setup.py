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
from setuptools import find_packages
from setuptools import setup

from clusterman import __version__

setup(
    name='clusterman',
    version=__version__,
    provides=['clusterman'],
    author='Compute Infrastructure',
    author_email='compute-infra+github@yelp.com',
    description='Distributed cluster scaling and management tools',
    packages=find_packages(exclude=['tests']),
    setup_requires=['setuptools'],
    include_package_data=True,
    install_requires=[
    ],
    scripts=[
        'clusterman/supervisord/fetch_clusterman_signal',
        'clusterman/supervisord/run_clusterman_signal',
    ],
    entry_points={
        'console_scripts': [
            'clusterman=clusterman.run:main',
        ],
        'static_completion': [
            'clusterman=clusterman.args:get_parser',
        ],
    },
)
