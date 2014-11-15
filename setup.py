import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.test_suite = 'tests'

    def run_tests(self):
        import pytest
        sys.exit(pytest.main(self.test_args))


setup(
    name="redrobin",
    version="0.1",
    description="Distributed roundrobin based on Redis",
    long_description=open("README.rst").read(),
    url="https://bitbucket.org/gsakkis/redrobin",
    author="George Sakkis",
    author_email="george.sakkis@gmail.com",
    packages=find_packages(),
    install_requires=["redis", "redis-collections"],
    tests_require=["pytest-cache", "pytest-cov", "pytest", "mock"],
    cmdclass={"test": PyTest},
    keywords="roundrobin throttling redis",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
    ],
)
