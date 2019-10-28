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
