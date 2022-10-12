from setuptools import setup
from glob import glob
import os

description = 'A simple work queue'

__version__ = None

version_file = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),
    'wq',
    'version.py'
)
with open(version_file, 'r') as fp:
    exec(fp.read())

scripts = glob('bin/*')
scripts = [s for s in scripts if '~' not in s]

setup(
    name='wq',
    version=__version__,
    description=description,
    url='https://github.com/esheldon/wq',
    author='Erin Scott Sheldon, Anze Slosar',
    author_email='erin.sheldon@gmail.com',
    scripts=scripts,
    packages=['wq'],
)
