# MIT License
#
# Copyright (c) 2020–2024 Brockmann Consult GmbH
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
import json
import os
import pathlib
from typing import Dict
from typing import List
from typing import Mapping
from typing import Optional
from typing import Tuple

import xarray as xr
from xcube.core.store import DataDescriptor
from xcube.core.store import DatasetDescriptor
from xcube.core.store import VariableDescriptor
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonDateSchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
import zipfile

from xcube_cds.store import CDSDatasetHandler


class ERA5DatasetHandler(CDSDatasetHandler):

    def __init__(self, api_version: int = 1):
        """Instantiate a new ERA5 dataset handler

        :param api_version: the API version to use when interfacing with the
            backend CDS service. 1 indicates the original API version used
            at launch. 2 indicates the new version introduced in 2024.
        """
        self._read_dataset_info()
        assert api_version in {1, 2}
        self._api_version = api_version

    def _read_dataset_info(self):
        """Read dataset information from JSON files"""

        # Information for each supported dataset is contained in a
        # semi-automatically generated JSON file. The largest part of this
        # file is the "variables" table. This table maps request parameter
        # names to NetCDF variable names, and was generated by the following
        # process:
        #
        # 1. Obtain the complete list of valid request parameters via the Web
        #    interface by selecting every box and copying the parameter names
        #    out of the generated API request.
        #
        # 2. For each request parameter, make a separate API request
        #    containing only that request parameter, producing a NetCDF file
        #    containing only the corresponding output parameter.
        #
        # 3. Read the name of the single output variable from the NetCDF file
        #    and collate it with the original request parameter. (Also read the
        #    long_name and units attributes.)
        #
        # In this way we are guaranteed to get the correct NetCDF variable
        # name for each request parameter, without having to trust that the
        # documentation is correct.
        #
        # Unfortunately this procedure doesn't work with all datasets, since
        # some (e.g. satellite-soil-moisture) don't have a one-to-one mapping
        # from request variables to output variables.
        #
        # Table fields are:
        # 1. request parameter name in CDS API
        # 2. NetCDF variable name (NB: not always CF-conformant)
        # 3. units from NetCDF attributes
        # 4. "long name" from NetCDF attributes

        ds_info_path = pathlib.Path(__file__).parent
        all_pathnames = [
            os.path.join(ds_info_path, leafname)
            for leafname in os.listdir(ds_info_path)
        ]
        pathnames = filter(
            lambda p: os.path.isfile(p) and p.endswith(".json"), all_pathnames
        )
        self._dataset_dicts = {}
        for pathname in pathnames:
            with open(pathname, "r") as fh:
                ds_dict = json.load(fh)
                _, leafname = os.path.split(pathname)
                self._dataset_dicts[leafname[:-5]] = ds_dict

        # The CDS API delivers data from these datasets in an unhelpful format
        # (issue #6) and sometimes with non-increasing time (issue #5), so
        # for now they are blacklisted.
        blacklist = frozenset(
            [
                "reanalysis-era5-land-monthly-means:"
                "monthly_averaged_reanalysis_by_hour_of_day",
                "reanalysis-era5-single-levels-monthly-means:"
                "monthly_averaged_ensemble_members_by_hour_of_day",
                "reanalysis-era5-single-levels-monthly-means:"
                "monthly_averaged_reanalysis_by_hour_of_day",
            ]
        )

        # We use a list rather than a set, since we want to preserve ordering
        # and the number of elements is small.
        self._valid_data_ids = []
        self._data_id_to_human_readable = {}
        for ds_id, ds_dict in self._dataset_dicts.items():
            # product_type is actually a request parameter, but we implement
            # it as a suffix to the data_id to make it possible to specify
            # requests using only the standard, known store parameters.
            product_types = ds_dict["product_types"]
            if len(product_types) == 0:
                # No product types defined (i.e. there is just a single,
                # implicit product type), so we just use the dataset ID without
                # a suffix.
                if ds_id not in blacklist:
                    self._valid_data_ids.append(ds_id)
                    self._data_id_to_human_readable[ds_id] = ds_dict["description"]
            else:
                for pt_id, pt_desc in product_types:
                    data_id = ds_id + ":" + pt_id
                    if data_id not in blacklist:
                        self._valid_data_ids.append(data_id)
                        self._data_id_to_human_readable[data_id] = (
                            ds_dict["description"] + " \N{EN DASH} " + pt_desc
                        )

    def get_supported_data_ids(self) -> List[str]:
        return list(self._valid_data_ids)

    def get_open_data_params_schema(
        self, data_id: Optional[str] = None
    ) -> JsonObjectSchema:
        # If the data_id has a product type suffix, remove it.
        dataset_id = data_id.split(":")[0] if ":" in data_id else data_id

        ds_info = self._dataset_dicts[dataset_id]
        variable_info_table = ds_info["variables"]
        bbox = ds_info["bbox"]

        params = dict(
            variable_names=JsonArraySchema(
                items=(
                    JsonStringSchema(
                        min_length=0,
                        enum=[
                            cds_api_name
                            for cds_api_name, _, _, _ in variable_info_table
                        ],
                    )
                ),
                unique_items=True,
                nullable=True,
                description="identifiers of the requested variables",
            ),
            # crs omitted, since it's constant.
            # W, S, E, N (will be converted to N, W, S, E)
            bbox=JsonArraySchema(
                items=(
                    JsonNumberSchema(minimum=bbox[0], maximum=bbox[2]),
                    JsonNumberSchema(minimum=bbox[1], maximum=bbox[3]),
                    JsonNumberSchema(minimum=bbox[0], maximum=bbox[2]),
                    JsonNumberSchema(minimum=bbox[1], maximum=bbox[3]),
                ),
                description="bounding box (min_x, min_y, max_x, max_y)",
            ),
            # spatial_res in the ds_info dictionary gives the minimum
            # resolution, but the ERA5 backend can resample, so we
            # also set a maximum. The choice of 10° as maximum is fairly
            # arbitrary but seems reasonable.
            spatial_res=JsonNumberSchema(
                minimum=ds_info["spatial_res"],
                maximum=10,
                description="spatial resolution",
            ),
            time_range=JsonDateSchema.new_range(),
            # time_period (time aggregation period) omitted, since it is
            # constant.
        )
        required = [
            "variable_names",
            "bbox",
            "time_range",
        ]
        return JsonObjectSchema(
            properties=params, required=required, additional_properties=False
        )

    def get_human_readable_data_id(self, data_id: str):
        return self._data_id_to_human_readable[data_id]

    def describe_data(self, data_id: str) -> DataDescriptor:
        ds_info = self._dataset_dicts[data_id.split(":")[0]]

        return DatasetDescriptor(
            data_id=data_id,
            data_vars=self._create_variable_descriptors(data_id),
            crs=ds_info["crs"],
            bbox=tuple(ds_info["bbox"]),
            spatial_res=ds_info["spatial_res"],
            time_range=tuple(ds_info["time_range"]),
            time_period=ds_info["time_period"],
            open_params_schema=self.get_open_data_params_schema(data_id),
        )

    def _create_variable_descriptors(
        self, data_id: str
    ) -> Mapping[str, VariableDescriptor]:
        dataset_id = data_id.split(":")[0]

        return {
            netcdf_name: VariableDescriptor(
                name=netcdf_name,
                # dtype string format not formally defined as of 2020-06-18.
                # t2m is actually stored as a short with scale and offset in
                # the NetCDF file, but converted to float by xarray on opening:
                # see http://xarray.pydata.org/en/stable/io.html .
                dtype="float32",
                dims=("time", "latitude", "longitude"),
                attrs=dict(units=units, long_name=long_name),
            )
            for (
                api_name,
                netcdf_name,
                units,
                long_name,
            ) in self._dataset_dicts[
                dataset_id
            ]["variables"]
        }

    def transform_params(self, plugin_params: Dict, data_id: str) -> Tuple[str, Dict]:
        """Transform supplied parameters to CDS API format.

        :param plugin_params: parameters in form expected by this plugin
        :param data_id: the ID of the requested dataset
        :return: parameters in form expected by the CDS API
        """

        dataset_name, product_type = (
            data_id.split(":") if ":" in data_id else (data_id, None)
        )

        # We need to split out the bounding box co-ordinates to re-order them.
        x1, y1, x2, y2 = plugin_params["bbox"]

        # Translate our parameters (excluding time parameters) to the CDS API
        # scheme.
        if "spatial_res" in plugin_params:
            resolution = plugin_params["spatial_res"]
        else:
            ds_info = self._dataset_dicts[data_id.split(":")[0]]
            resolution = ds_info["spatial_res"]

        variable_names_param = plugin_params["variable_names"]
        # noinspection PySimplifyBooleanCheck
        if variable_names_param == []:
            # The "empty list of variables" case should be handled by the main
            # store class; if an empty list gets this far, something's wrong.
            raise ValueError("variable_names may not be an empty list.")
        elif variable_names_param is None:
            variable_table = self._dataset_dicts[dataset_name]["variables"]
            variable_names = [line[0] for line in variable_table]
        else:
            variable_names = variable_names_param

        params_combined = {
            "variable": variable_names,
            # For the ERA5 dataset, we need to crop the area by half a
            # cell-width. ERA5 data are points, but xcube treats them as
            # cell centres. The bounds of a grid of cells are half a cell-width
            # outside the bounds of a grid of points, so we have to crop each
            # edge by half a cell-width to end up with the requested bounds.
            # See https://confluence.ecmwf.int/display/CKB/ERA5%3A+What+is+the+spatial+reference#ERA5:Whatisthespatialreference-Visualisationofregularlat/londata
            "area": [
                y2 - resolution / 2,
                x1 + resolution / 2,
                y1 + resolution / 2,
                x2 - resolution / 2,
            ],
            # API versions 1 and 2 use different keys for format specifier.
            {1: "format", 2: "data_format"}[self._api_version]: "netcdf",
        }

        # Note: the "grid" parameter is not exposed via the web interface,
        # but is described at
        # https://confluence.ecmwf.int/display/CKB/ERA5%3A+Web+API+to+CDS+API
        if "spatial_res" in plugin_params:
            params_combined["grid"] = [resolution, resolution]

        if product_type is not None:
            params_combined["product_type"] = product_type

        # Convert the time range specification to the nearest equivalent
        # in the CDS "orthogonal time units" scheme.
        time_params_from_range = self.transform_time_params(
            self.convert_time_range(plugin_params["time_range"])
        )
        params_combined.update(time_params_from_range)

        # If any of the "years", "months", "days", and "hours" parameters
        # were passed, they override the time specifications above.
        time_params_explicit = self.transform_time_params(plugin_params)
        params_combined.update(time_params_explicit)

        # Transform singleton list values into their single members, as
        # required by the CDS API.
        desingletonned = self.unwrap_singleton_values(params_combined)

        return dataset_name, desingletonned

    def read_file(
        self,
        dataset_name: str,
        open_params: Dict,
        cds_api_params: Dict,
        file_path: str,
        temp_dir: str,
    ):
        # decode_cf=True is the default and the netcdf4 engine should be
        # available and automatically selected, but it's safer and clearer to
        # be explicit.
        if zipfile.is_zipfile(file_path):
            path_temp = os.path.join(pathlib.Path(file_path).parent.resolve(), "temp")
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(path_temp)
            file_paths = glob.glob(f"{path_temp}/*")
            ds = xr.open_mfdataset(file_paths, engine="netcdf4")
        else:
            ds = xr.open_dataset(file_path, engine="netcdf4", decode_cf=True)
        return ds
