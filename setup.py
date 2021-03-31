from setuptools import setup, find_packages
import re
import os

BASEDIR = os.path.dirname(os.path.abspath(__file__))

VERSION_RE = re.compile(r'''__version__ = ['"]([0-9.]+)['"]''')

def get_version():
    init = open(os.path.join(BASEDIR, 'honeycomb_io', '__init__.py')).read()
    return VERSION_RE.search(init).group(1)

# Dependencies (format is 'PYPI_PACKAGE_NAME[>]=VERSION_NUMBER')
BASE_DEPENDENCIES = [
    'wf-minimal-honeycomb-python>=0.6.0',
    'pandas>=1.2.2',
    'numpy>=1.20.1',
    'boto3>=1.17',
    'inflection>=0.5.1'
]

# TEST_DEPENDENCIES = [
# ]

# DEVELOPMENT_DEPENDENCIES = [
# ]

# LOCAL_DEPENDENCIES = [
# ]

# Allow setup.py to be run from any path
os.chdir(os.path.normpath(BASEDIR))

setup(
    name='wf-honeycomb-io',
    packages=find_packages(),
    version=get_version(),
    include_package_data=True,
    description='Tools for working with data in Wildflower\'s Honeycomb database',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/WildflowerSchools/wf-honeycomb-io',
    author='Theodore Quinn',
    author_email='ted.quinn@wildflowerschools.org',
    install_requires=BASE_DEPENDENCIES,
    # tests_require=TEST_DEPENDENCIES,
    # extras_require = {
    #     'test': TEST_DEPENDENCIES,
    #     'development': DEVELOPMENT_DEPENDENCIES,
    #     'local': LOCAL_DEPENDENCIES
    # },
    # entry_points={
    #     "console_scripts": [
    #          "COMMAND_NAME = MODULE_PATH:METHOD_NAME"
    #     ]
    # },
    keywords=['database'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ]
)
