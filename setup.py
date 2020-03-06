# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='auto_test_analytics',  # Required
    version='0.1.0',  # Required
    description='Auto tests for Analytics',  # Optional
    long_description=long_description,  # Optional
    long_description_content_type='text/markdown',  # Optional (see note above)
    classifiers=[
        'Programming Language :: Python :: 3',
    ],
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),  # Required
    python_requires='>=3.5, <4',  # Required
    install_requires=['pandas', 'pymongo', 'requests', 'flake8', 'bson', 'python-keycloak'],  # Optional
    entry_points={  # Optional
        'console_scripts': [
            'auto_tests=auto_tests:main',
        ],
    }
)
