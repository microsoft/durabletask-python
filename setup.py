# -*- coding: utf-8 -*-

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="durabletask",
    version="0.1.0-alpha.1",
    author="Chris Gillum",
    author_email="cgillum@microsoft.com",
    description="Durable Task Framework for Python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/microsoft/durabletask-python",
    packages=setuptools.find_packages(),
    classifiers=[
        'Development Status :: 3 - Alpha',
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)