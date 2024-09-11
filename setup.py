#!/usr/bin/env python3

# MIT License
#
# Copyright (c) 2020-2024 Brockmann Consult GmbH
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from setuptools import setup, find_packages

requirements = [
    # Requirements are listed in ./environment.yml.
    # Since xcube is a requirement, and since xcube is not currently being
    # released on PyPI, the requirements list is omitted here. It is expected
    # that dependencies will be managed by conda.
]

packages = find_packages(exclude=["test", "test.*"])

# Same effect as "from xcube_cds import version" but avoids importing xcube_cds
version = None
with open("xcube_cds/version.py") as f:
    exec(f.read())

setup(
    name="xcube_cds",
    version=version,
    description="An xcube plugin to generate data cubes from the "
    "Climate Data Store (CDS) API",
    license="MIT",
    author="xcube Development Team",
    packages=packages,
    package_data={
        "xcube_cds.datasets": [
            "reanalysis-era5-land.json",
            "reanalysis-era5-land-monthly-means.json",
            "reanalysis-era5-single-levels.json",
            "reanalysis-era5-single-levels-monthly-means.json",
        ]
    },
    install_requires=requirements,
)
