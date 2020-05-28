from setuptools import setup

description = "A simple work queue"

setup(
    name="wq",
    version="0.2.0",
    description=description,
    url="https://github.com/esheldon/wq",
    author="Erin Scott Sheldon, Anze Slosar",
    author_email="erin.sheldon@gmail.com",
    scripts=["bin/wq"],
    packages=['wq'],
)
