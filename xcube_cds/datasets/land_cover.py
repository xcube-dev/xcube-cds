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


import numpy as np
from xcube.core.store import DatasetDescriptor, VariableDescriptor
from xcube.util.jsonschema import (
    JsonArraySchema,
    JsonDateSchema,
    JsonNumberSchema,
    JsonObjectSchema,
)

from xcube_cds.store import CDSDatasetHandler
from xcube_cds.utils import _read_file


class LandCoverDatasetHandler(CDSDatasetHandler):

    def __init__(self):
        self._data_id_map = {
            "satellite-land-cover": (
                "Land cover classification gridded maps from 1992 to present "
                "derived from satellite observations"
            )
        }
        self._min_date = "1992-01-01"
        self._max_date = "2022-12-31"
        self._bbox = (-180.0, -90.0, 180.0, 90.0)
        self._spatial_res = 0.00277778

    def get_supported_data_ids(self) -> list[str]:
        return list(self._data_id_map)

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
        )
        required = ["time_range"]
        return JsonObjectSchema(
            properties=params, required=required, additional_properties=False
        )

    def get_human_readable_data_id(self, data_id: str) -> str:
        return self._data_id_map[data_id]

    def describe_data(self, data_id: str) -> DatasetDescriptor:
        metadata_vars = self._get_metadata_vars()
        variable_descriptors = [
            VariableDescriptor(
                name=var_name,
                dtype=dtype,
                dims=("time", "lat", "lon"),
                attrs=attrs,
            )
            for (var_name, dtype, attrs) in metadata_vars
        ]
        return DatasetDescriptor(
            data_id=data_id,
            data_vars={desc.name: desc for desc in variable_descriptors},
            crs="EPSG:4326",
            bbox=self._bbox,
            spatial_res=self._spatial_res,
            time_range=(self._min_date, self._max_date),
            time_period="1Y",
            open_params_schema=self.get_open_data_params_schema(data_id),
        )

    def transform_params(self, plugin_params: dict, data_id: str) -> tuple[str, dict]:
        year_start = int(plugin_params["time_range"][0][:4])
        year_end = int(plugin_params["time_range"][1][:4])

        params_combined = {
            "variable": "all",
            "year": list(np.arange(year_start, year_end + 1).astype("str")),
            "version": ["v2_0_7cds", "v2_1_1"],
        }
        if "bbox" in plugin_params:
            params_combined["area"] = [
                plugin_params["bbox"][3],
                plugin_params["bbox"][0],
                plugin_params["bbox"][1],
                plugin_params["bbox"][2],
            ]

        return data_id, params_combined

    def read_file(
        self,
        dataset_name: str,
        open_params: dict,
        cds_api_params: dict,
        file_path: str,
        temp_dir: str,
    ):
        return _read_file(file_path)

    @staticmethod
    def _get_metadata_vars() -> tuple:
        return (
            (
                "lccs_class",
                "uint8",
                {
                    "standard_name": "land_cover_lccs",
                    "flag_colors": (
                        "#ffff64 #ffff64 #ffff00 #aaf0f0 #dcf064 #c8c864 #006400 "
                        "#00a000 #00a000 #aac800 #003c00 #003c00 #005000 #285000 "
                        "#285000 #286400 #788200 #8ca000 #be9600 #966400 #966400 "
                        "#966400 #ffb432 #ffdcd2 #ffebaf #ffc864 #ffd278 #ffebaf "
                        "#00785a #009678 #00dc82 #c31400 #fff5d7 #dcdcdc #fff5d7 "
                        "#0046c8 #ffffff"
                    ),
                    "long_name": "Land cover class defined in LCCS",
                    "valid_min": 1,
                    "valid_max": 220,
                    "ancillary_variables": (
                        "processed_flag current_pixel_state "
                        "observation_count change_count"
                    ),
                    "flag_meanings": (
                        "no_data cropland_rainfed cropland_rainfed_herbaceous_cover "
                        "cropland_rainfed_tree_or_shrub_cover cropland_irrigated "
                        "mosaic_cropland mosaic_natural_vegetation "
                        "tree_broadleaved_evergreen_closed_to_open "
                        "tree_broadleaved_deciduous_closed_to_open "
                        "tree_broadleaved_deciduous_closed "
                        "tree_broadleaved_deciduous_open "
                        "tree_needleleaved_evergreen_closed_to_open "
                        "tree_needleleaved_evergreen_closed "
                        "tree_needleleaved_evergreen_open "
                        "tree_needleleaved_deciduous_closed_to_open "
                        "tree_needleleaved_deciduous_closed "
                        "tree_needleleaved_deciduous_open "
                        "tree_mixed mosaic_tree_and_shrub "
                        "mosaic_herbaceous shrubland shrubland_evergreen "
                        "shrubland_deciduous grassland lichens_and_mosses "
                        "sparse_vegetation sparse_tree sparse_shrub sparse_herbaceous "
                        "tree_cover_flooded_fresh_or_brakish_water "
                        "tree_cover_flooded_saline_water "
                        "shrub_or_herbaceous_cover_flooded urban bare_areas "
                        "bare_areas_consolidated bare_areas_unconsolidated "
                        "water snow_and_ice"
                    ),
                    "flag_values": [
                        0,
                        10,
                        11,
                        12,
                        20,
                        30,
                        40,
                        50,
                        60,
                        61,
                        62,
                        70,
                        71,
                        72,
                        80,
                        81,
                        82,
                        90,
                        100,
                        110,
                        120,
                        121,
                        122,
                        130,
                        140,
                        150,
                        151,
                        152,
                        153,
                        160,
                        170,
                        180,
                        190,
                        200,
                        201,
                        202,
                        210,
                        220,
                    ],
                },
            ),
            (
                "processed_flag",
                "float32",  # should be int but output is float32
                {
                    "long_name": "LC map processed area flag",
                    "standard_name": "land_cover_lccs status_flag",
                    "valid_min": 0,
                    "valid_max": 1,
                    "flag_meanings": "not_processed processed",
                    "flag_values": [0, 1],
                },
            ),
            (
                "current_pixel_state",
                "float32",  # should be int but output is float32
                {
                    "long_name": "LC pixel type mask",
                    "standard_name": "land_cover_lccs status_flag",
                    "valid_min": 0,
                    "valid_max": 5,
                    "flag_meanings": (
                        "invalid clear_land clear_water "
                        "clear_snow_ice cloud cloud_shadow"
                    ),
                    "flag_values": [0, 1, 2, 3, 4, 5],
                },
            ),
            (
                "observation_count",
                "int16",
                {
                    "long_name": "number of valid observations",
                    "standard_name": "land_cover_lccs number_of_observations",
                    "valid_min": 0,
                    "valid_max": 32767,
                },
            ),
            (
                "change_count",
                "uint8",
                {
                    "long_name": "number of class changes",
                    "valid_min": 0,
                    "valid_max": 100,
                },
            ),
        )
