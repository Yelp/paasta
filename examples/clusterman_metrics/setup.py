from setuptools import find_packages
from setuptools import setup


setup(
    name='clusterman-metrics',
    version='1.0.0',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],
    package_data={str('clusterman_metrics'): [str('py.typed')]},
    install_requires=[
        'boto3',
        'PyStaticConfiguration',
    ],
    packages=find_packages(exclude=('tests*', 'testing*')),
    zip_safe=False,
)
