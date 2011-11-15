import distutils
from distutils.core import setup

description = "A simple work queue that actually works."

setup(name="awq", 
      version="0.1.0",
      description=description,
      url="https://github.com/esheldon/awq",
      author="Erin Scott Sheldon, Anze Slosar",
      author_email="erin.sheldon@gmail.com",
      packages=['awq'])




