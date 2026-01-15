# MIT License
#
# Copyright (c) 2020–2025 Brockmann Consult GmbH
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

import glob
import os
import pathlib
import zipfile

import xarray as xr


def _read_file(file_path: str):
    # decode_cf=True is the default and the netcdf4 engine should be
    # available and automatically selected, but it's safer and clearer to
    # be explicit.
    if zipfile.is_zipfile(file_path):
        path_temp = os.path.join(pathlib.Path(file_path).parent.resolve(), "temp")
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(path_temp)
        file_paths = glob.glob(f"{path_temp}/*")
        ds = xr.open_mfdataset(
            file_paths, engine="netcdf4", chunks="auto", decode_cf=True
        )
    else:
        ds = xr.open_dataset(file_path, engine="netcdf4", chunks="auto", decode_cf=True)
    return ds
