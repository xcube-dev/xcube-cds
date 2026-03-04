# MIT License
#
# Copyright (c) 2026 Brockmann Consult GmbH
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
from typing import Dict, Union, List, Tuple, Any

import xarray as xr
from xcube.core.store import DatasetDescriptor, VariableDescriptor
from xcube.util.jsonschema import (
    JsonArraySchema,
    JsonDateSchema,
    JsonNumberSchema,
    JsonStringSchema,
    JsonObjectSchema,
)

from xcube_cds.store import CDSDatasetHandler
from xcube_cds.utils import _read_file


_LST_DATASET_NAME = "satellite-land-surface-temperature"


class LandSurfaceTemperatureDatasetHandler(CDSDatasetHandler):

    def __init__(self):
        self._data_id_map = {
            _LST_DATASET_NAME:
                "Land surface temperature monthly gridded data from 1995 to present derived from satellite observations"
        }
        self._bbox = (-180.0, -90.0, 180.0, 90.0)
        self._min_date = "1995-06-01"
        self._max_date = "2025-06-30"

    def get_supported_data_ids(self) -> List[str]:
        return list(self._data_id_map.keys())

    def get_open_data_params_schema(
        self, data_id: str | None = None
    ) -> JsonObjectSchema:
        params = dict(
            bbox=JsonArraySchema(
                items=(
                    JsonNumberSchema(minimum=self._bbox[0], maximum=self._bbox[2]),
                    JsonNumberSchema(minimum=self._bbox[1], maximum=self._bbox[3]),
                    JsonNumberSchema(minimum=self._bbox[0], maximum=self._bbox[2]),
                    JsonNumberSchema(minimum=self._bbox[1], maximum=self._bbox[3]),
                ),
                description="bounding box (min_x, min_y, max_x, max_y)",
                default=self._bbox,
            ),
            time_range=JsonDateSchema.new_range(
                min_date=self._min_date, max_date=self._max_date
            ),
            observation_time=JsonStringSchema(
                enum=["day", "night"],
                title="Observation Time",
                description=(
                    "The land surface temperature data are split into day and night products using the "
                    "solar zenith angle (daytime when solar zenith angle < 90° "
                    "and nighttime when solar zenith angle > 90°). "
                ),
                default="day",
            ),
        )
        required = ["time_range"]
        return JsonObjectSchema(
            properties=params, required=required, additional_properties=False
        )

    def _validate_data_id(self, data_id: str):
        if data_id not in self.get_supported_data_ids():
            raise ValueError(f"Data id '{data_id}' not provided by Land Surface Temperature Handler")

    def get_human_readable_data_id(self, data_id: str) -> str:
        self._validate_data_id(data_id)
        return self._data_id_map[data_id]

    def describe_data(self, data_id: str) -> DatasetDescriptor:
        self._validate_data_id(data_id)
        descriptors = [
            VariableDescriptor(
                name="dtime",
                dtype="float64",
                dims=("time", "lat", "lon"),
                attrs={"long_name": "time difference from reference time"},
            ),
            VariableDescriptor(
                name="satze",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "satellite zenith angle",
                    "units": "degrees"
                },
            ),
            VariableDescriptor(
                name="sataz",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "satellite azimuth angle",
                    "units": "degrees"
                },
            ),
            VariableDescriptor(
                name="solze",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "solar zenith angle",
                    "units": "degrees"
                },
            ),
            VariableDescriptor(
                name="solaz",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "solar azimuth angle",
                    "units": "degrees"
                },
            ),
            VariableDescriptor(
                name="lst",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "land surface temperature",
                    "units": "kelvin",
                    "valid_min": -8315,
                    "valid_max": 7685
                },
            ),
            VariableDescriptor(
                name="lst_uncertainty",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "land surface temperature",
                    "valid_min": 0,
                    "valid_max": 10000
                },
            ),
            VariableDescriptor(
                name="lst_uncertainty",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "land surface temperature total uncertainty",
                    "units": "kelvin",
                    "valid_min": 0,
                    "valid_max": 10000
                },
            ),
            VariableDescriptor(
                name="lst_unc_ran",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "uncertainty from uncorrelated errors",
                    "units": "kelvin",
                    "valid_min": 0,
                    "valid_max": 10000
                },
            ),
            VariableDescriptor(
                name="lst_unc_loc_atm",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "uncertainty from locally correlated errors on atmospheric scales",
                    "units": "kelvin",
                    "valid_min": 0,
                    "valid_max": 10000
                },
            ),
            VariableDescriptor(
                name="lst_unc_loc_sfc",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "uncertainty from locally correlated errors on surface scales",
                    "units": "kelvin",
                    "valid_min": 0,
                    "valid_max": 10000
                },
            ),
            VariableDescriptor(
                name="lst_unc_sys",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "uncertainty from large-scale systematic errors",
                    "units": "kelvin",
                    "valid_min": 0,
                    "valid_max": 10000
                },
            ),
            VariableDescriptor(
                name="lcc",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "land cover class",
                    "units": "kelvin",
                    "valid_min": 10,
                    "valid_max": 230,
                    "flag_meanings": [
                        "cropland_rainfed",
                        "cropland_rainfed_herbaceous_cover",
                        "cropland_rainfed_tree_or_shrub_cover",
                        "cropland_irrigated",
                        "mosaic_cropland",
                        "mosaic_natural_vegetation",
                        "tree_broadleaved_evergreen_closed_to_open",
                        "tree_broadleaved_deciduous_closed_to_open",
                        "tree_broadleaved_deciduous_closed",
                        "tree_broadleaved_deciduous_open",
                        "tree_needleleaved_evergreen_closed_to_open",
                        "tree_needleleaved_evergreen_closed",
                        "tree_needleleaved_evergreen_open"
                        "tree_needleleaved_deciduous_closed_to_open",
                        "tree_needleleaved_deciduous_closed",
                        "tree_needleleaved_deciduous_open",
                        "tree_mixed",
                        "mosaic_tree_and_shrub",
                        "mosaic_herbaceous",
                        "shrubland",
                        "shrubland_evergreen",
                        "shrubland_deciduous",
                        "grassland",
                        "lichens_and_mosses",
                        "sparse_vegetation",
                        "sparse_tree",
                        "sparse_shrub",
                        "sparse_herbaceous",
                        "tree_cover_flooded_fresh_or_brakish_water",
                        "tree_cover_flooded_saline_water",
                        "shrub_or_herbaceous_cover_flooded",
                        "urban"
                        "Bare_areas_of_soil_types_not_contained_in_biomes_203_to_207",
                        "Unconsolidated_bare_areas_of_soil_types_not_contained_in_biomes_203_to_207",
                        "Consolidated_bare_areas_of_soil_types_not_contained_in_biomes_203_to_207",
                        "Bare_areas_of_soil_type_Entisols_Orthents",
                        "Bare_areas_of_soil_type_Shifting_sand",
                        "Bare_areas_of_soil_type_Aridisols_Calcids",
                        "Bare_areas_of_soil_type_Aridisols_Cambids",
                        "Bare_areas_of_soil_type_Gelisols_Orthels water snow_and_ice Sea_ice"
                    ],
                    "flag_values": [
                        10, 11, 12, 20, 30, 40, 50, 60, 61, 62, 70, 71, 72, 80, 81, 82, 90, 100,
                        110, 120, 121, 122, 130, 140, 150, 151, 152, 153, 160, 170, 180, 190, 200,
                        201, 202, 203, 204, 205, 206, 207, 210, 220, 230
                    ]
                },
            ),
            VariableDescriptor(
                name="n",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "number of clear-sky pixels",
                    "valid_min": 0,
                    "valid_max": 18750
                },
            ),
            VariableDescriptor(
                name="lst_unc_loc_cor",
                dtype="float32",
                dims=("time", "lat", "lon"),
                attrs={
                    "long_name": "uncertainty from locally correlated errors on LST corrections",
                    "units": "kelvin",
                    "valid_min": 0,
                    "valid_max": 10000
                },
            ),
        ]

        return DatasetDescriptor(
            data_id=data_id,
            data_vars={desc.name: desc for desc in descriptors},
            crs="WGS84",
            bbox=self._bbox,
            spatial_res=0.01,
            time_range=(self._min_date, self._max_date),
            time_period="1M",
            open_params_schema=self.get_open_data_params_schema(data_id),
        )

    def transform_params(self, opener_params: Dict, data_id: str) -> Tuple[str, Dict[str, Any]]:
        time_params_from_range = self.transform_time_params(
            self.convert_time_range(opener_params["time_range"])
        )

        observation_time = opener_params.get("observation_time", "day")

        cds_params = dict(
            variable="land_surface_temperature",
            observation_time=observation_time,
            year=time_params_from_range["year"],
            month=time_params_from_range["month"],
            version="v3_00"
        )

        if "bbox" in opener_params:
            cds_params["area"] = [
                opener_params["bbox"][3],
                opener_params["bbox"][0],
                opener_params["bbox"][1],
                opener_params["bbox"][2],
            ]

        # Transform singleton list values into their single members, as
        # required by the CDS API.
        unwrapped = self.unwrap_singleton_values(cds_params)

        return _LST_DATASET_NAME, unwrapped

    def read_file(self, dataset_name: str, open_params: Dict, cds_api_params: Dict[str, Union[str, List[str]]],
                  file_path: str, temp_dir: str) -> xr.Dataset:
         ds = _read_file(file_path, temp_dir)
         if "time_range" in open_params:
             start_time, end_time = open_params["time_range"]
             for time_var_name in "time", "valid_time":
                 if time_var_name in ds.coords:
                     ds = ds.sel({time_var_name: slice(start_time, end_time)})
         return ds
