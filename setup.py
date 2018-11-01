import sys
from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(
    name='Simperium3',
    version='0.1.3',
    author='Andy Gayton',
    author_email='andy@simperium.com',
    packages=['simperium', 'simperium.test'],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/swalladge/simperium-python3",
    # license='LICENSE.txt',
    description='Python 3 client for the Simperium synchronization platform',
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=['requests', 'typing;python_version<"3.5"'],
    python_requires='>=3',
)
