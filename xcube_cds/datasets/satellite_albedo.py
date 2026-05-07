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

import collections
from typing import Dict, Union, List, Tuple, Any
import xarray as xr

from xcube.core.store import DatasetDescriptor, VariableDescriptor
from xcube.util.jsonschema import (
    JsonObjectSchema,
    JsonArraySchema,
    JsonNumberSchema,
    JsonDateSchema,
    JsonStringSchema,
)

from xcube_cds.store import CDSDatasetHandler
from xcube_cds.utils import _read_file

SensorProperties = collections.namedtuple(
    "SensorProperties",
    [
        "human_readable_name",
        "long_name",
        "satellites",
        "product_versions",
        "resolution",
        "start_date",
        "end_date",
        "descriptors",
    ],
)


def _var(name, long_name, dtype="float32", valid_min=0):
    return VariableDescriptor(
        name=name,
        dtype=dtype,
        dims=("time", "lat", "lon"),
        attrs={
            "long_name": long_name,
            "grid_mapping": "crs",
            "standard_name": "surface_albedo",
            "units": "1",
            "valid_min": valid_min,
            "valid_max": 10000,
        },
    )


def _flag_var(
    name, long_name, dtype, flag_meanings, flag_masks, flag_values, valid_range
):
    return VariableDescriptor(
        name=name,
        dtype=dtype,
        dims=("time", "lat", "lon"),
        attrs={
            "long_name": long_name,
            "flag_meanings": flag_meanings,
            "flag_masks": flag_masks,
            "flag_values": flag_values,
            "grid_mapping": "crs",
            "standard_name": "surface_albedo_status_flag",
            "units": "1",
            "valid_range": valid_range,
        },
    )


_TEXT = {"DH": "directional", "BH": "hemispherical"}


def _spectral_vars(channels, identifier, valid_min=0):
    return [
        var
        for ch, label in channels
        for var in [
            _var(
                f"AL_{identifier}_{ch}",
                f"Spectral {_TEXT[identifier]} albedo over {label} channel",
                valid_min=valid_min,
            ),
            _var(
                f"AL_{identifier}_{ch}_ERR",
                f"Uncertainty on spectral {_TEXT[identifier]} albedo over {label} channel",
                valid_min=valid_min,
            ),
        ]
    ]


def _broadband_vars(identifier, valid_min=0):
    return [
        _var(
            f"AL_{identifier}_VI",
            f"Broadband {_TEXT[identifier]} albedo over visible spectrum",
            valid_min=valid_min,
        ),
        _var(
            f"AL_{identifier}_NI",
            f"Broadband {_TEXT[identifier]} albedo over near-infrared spectrum",
            valid_min=valid_min,
        ),
        _var(
            f"AL_{identifier}_BB",
            f"Broadband {_TEXT[identifier]} albedo over total spectrum",
            valid_min=valid_min,
        ),
        _var(
            f"AL_{identifier}_NI_ERR",
            f"Uncertainty on broadband {_TEXT[identifier]} albedo over near-infrared spectrum",
        ),
        _var(
            f"AL_{identifier}_BB_ERR",
            f"Uncertainty on broadband {_TEXT[identifier]} albedo over total spectrum",
        ),
        _var(
            f"AL_{identifier}_VI_ERR",
            f"Uncertainty on broadband {_TEXT[identifier]} albedo over visible channel",
        ),
    ]


_AVHRR_AND_VGT_CHANNELS = [
    ("B0", "blue channel"),
    ("B2", "red channel"),
    ("B3", "near-infrared channel"),
    ("MIR", "mid-infrared channel"),
]

_S3_CHANNELS = [
    ("Oa03", "Sentinel-3 Oa03"),
    ("Oa04", "Sentinel-3 Oa04"),
    ("Oa07", "Sentinel-3 Oa07"),
    ("Oa17", "Sentinel-3 Oa17"),
    ("Oa21", "Sentinel-3 Oa21"),
    ("S1", "Sentinel-3 S1"),
    ("S2", "Sentinel-3 S2"),
    ("S5", "Sentinel-3 S5"),
    ("S6", "Sentinel-3 S6"),
]

_AVHRR_AND_VGT_DESCRIPTORS = [
    VariableDescriptor(
        name="AGE",
        dtype="float32",
        dims=("time", "lat", "lon"),
        attrs={
            "grid_mapping": "crs",
            "standard_name": "surface_albedo_mean_age_of_observations",
            "units": "1",
            "valid_min": 0,
            "valid_max": 127,
        },
    ),
    VariableDescriptor(
        name="NMOD",
        dtype="float32",
        dims=("time", "lat", "lon"),
        attrs={
            "grid_mapping": "crs",
            "standard_name": "surface_albedo_number_of_observations",
            "units": "1",
            "valid_min": 0,
            "valid_max": 20,
        },
    ),
]

_S3_DESCRIPTORS = [
    _flag_var(
        name="QFLAG",
        dtype="float64",
        long_name="Quality flag on broadband hemispherical albedo 300m",
        flag_meanings=[
            "snow_presence",
            *[f"no_obs_in_last_decade_for_{b[0]}" for b in _S3_CHANNELS],
            *[f"brdf_warning_{b[0]}" for b in _S3_CHANNELS],
        ],
        flag_masks=[
            1,
            2,
            4,
            8,
            16,
            32,
            64,
            128,
            256,
            512,
            1024,
            2048,
            4096,
            8192,
            16384,
            32768,
            65536,
            131072,
            262144,
        ],
        flag_values=[
            1,
            2,
            4,
            8,
            16,
            32,
            64,
            128,
            256,
            512,
            1024,
            2048,
            4096,
            8192,
            16384,
            32768,
            65536,
            131072,
            262144,
        ],
        valid_range=[0, 524287],
    ),
]

_AVHRR_DESCRIPTORS = [
    _flag_var(
        name="QFLAG",
        dtype="float32",
        long_name="Quality flag on broadband hemispherical albedo 4km",
        flag_meanings=[
            "sea",
            "land",
            "unused",
            "continental_water",
            "missing_input",
            "snow_presence",
            "albedo_processed_failure",
        ],
        flag_masks=[3, 3, 3, 3, 16, 32, 128],
        flag_values=[0, 1, 2, 3, 16, 32, 128],
        valid_range=[0, 255],
    ),
    *_AVHRR_AND_VGT_DESCRIPTORS,
]

_VGT_DESCRIPTORS = [
    _flag_var(
        name="QFLAG",
        dtype="float32",
        long_name="Quality flag on broadband hemispherical albedo 1km",
        flag_meanings=[
            "sea",
            "land",
            "unused",
            "continental_water",
            "missing_input",
            "snow_presence",
            "albedo_processed_failure",
        ],
        flag_masks=[3, 3, 3, 3, 16, 32, 128],
        flag_values=[0, 1, 2, 3, 16, 32, 128],
        valid_range=[0, 255],
    ),
    *_AVHRR_AND_VGT_DESCRIPTORS,
]

_DATASET_NAME = "satellite-albedo"
_SENSOR_MAP = {
    "avhrr": SensorProperties(
        "AVHRR",
        "AVHRR (Advanced Very High Resolution Radiometer)",
        ["noaa_7", "noaa_9", "noaa_11", "noaa_14", "noaa_16", "noaa_17"],
        ["v1", "v2"],
        "4km",
        "1981-09-20",
        "2005-12-31",
        _AVHRR_DESCRIPTORS,
    ),
    "vgt": SensorProperties(
        "VGT",
        "VGT (Vegetation)",
        ["proba", "spot"],
        ["v0", "v1", "v2"],
        "1km",
        "1998-04-10",
        "2020-06-30",
        _VGT_DESCRIPTORS,
    ),
    "olci_and_slstr": SensorProperties(
        "OLCI and SLSTR",
        "OLCI (Ocean and Land Color Instrument) and SLSTR (Sea and Land Surface Temperature Radiometer)",
        ["sentinel_3"],
        ["v3", "v3_1"],
        "300m",
        "2018-07-10",
        "2014-12-31",
        _S3_DESCRIPTORS,
    ),
}

_VARIABLE_NAMES = {
    "albb_bh": "Broadband Hemispherical",
    "albb_dh": "Broadband Directional",
    "alsp_bh": "Spectral Hemispherical",
    "alsp_dh": "Spectral Directional",
}


class AlbedoHandler(CDSDatasetHandler):

    def __init__(self):
        self._data_id_map = {}
        for var_name, var_description in _VARIABLE_NAMES.items():
            for sensor_name, sensor_vars in _SENSOR_MAP.items():
                self._data_id_map[
                    f"{_DATASET_NAME}:{sensor_name}:{var_name}"
                ] = (
                    f"Surface albedo 10-daily gridded data from 1981 to present ("
                    f"{sensor_vars.human_readable_name} {var_description})"
                )
        self._bbox = (-180.0, -90.0, 180.0, 90.0)

    def get_supported_data_ids(self) -> List[str]:
        return list(self._data_id_map.keys())

    def get_open_data_params_schema(self, data_id: str) -> JsonObjectSchema:
        self._validate_data_id(data_id)
        sensor = data_id.split(":")[1]
        sensor_params = _SENSOR_MAP[sensor]
        params = dict(
            bbox=JsonArraySchema(
                items=(
                    JsonNumberSchema(
                        minimum=self._bbox[0], maximum=self._bbox[2]
                    ),
                    JsonNumberSchema(
                        minimum=self._bbox[1], maximum=self._bbox[3]
                    ),
                    JsonNumberSchema(
                        minimum=self._bbox[0], maximum=self._bbox[2]
                    ),
                    JsonNumberSchema(
                        minimum=self._bbox[1], maximum=self._bbox[3]
                    ),
                ),
                description="bounding box (min_x, min_y, max_x, max_y)",
                default=self._bbox,
            ),
            time_range=JsonDateSchema.new_range(
                min_date=sensor_params.start_date,
                max_date=sensor_params.end_date,
            ),
            satellites=JsonArraySchema(
                items=(JsonStringSchema(enum=sensor_params.satellites)),
                min_items=1,
                default=sensor_params.satellites,
            ),
            product_versions=JsonArraySchema(
                items=(JsonStringSchema(enum=sensor_params.product_versions)),
                default=[sensor_params.product_versions[-1]],
            ),
        )
        required = ["time_range"]
        return JsonObjectSchema(
            properties=params, required=required, additional_properties=False
        )

    def get_human_readable_data_id(self, data_id: str) -> str:
        self._validate_data_id(data_id)
        return self._data_id_map[data_id]

    def _validate_data_id(self, data_id: str):
        if data_id not in self.get_supported_data_ids():
            supported_data_ids = ", ".join(self.get_supported_data_ids())
            raise ValueError(
                f"Data id '{data_id}' not provided by Albedo Handler. "
                f"Supported data ids: {supported_data_ids}."
            )

    def describe_data(self, data_id: str) -> DatasetDescriptor:
        self._validate_data_id(data_id)
        _, sensor, var_name = data_id.split(":")
        sensor_params = _SENSOR_MAP[sensor]

        channels = (
            _S3_CHANNELS
            if sensor == "olci_and_slstr"
            else _AVHRR_AND_VGT_CHANNELS
        )
        valid_min = -1000 if sensor == "avhrr" else 0

        var_descriptors = sensor_params.descriptors.copy()
        var_name = data_id.split(":")[2]
        var_type_1, var_type_2 = var_name.split("_")
        if var_type_1 == "albb":
            var_descriptors.extend(
                _broadband_vars(var_type_2.upper(), valid_min=valid_min)
            )
        elif var_type_1 == "alsp":
            var_descriptors.extend(
                _spectral_vars(
                    channels, var_type_2.upper(), valid_min=valid_min
                )
            )

        coord_descriptors = [
            VariableDescriptor(
                name="time",
                dtype="float64",
                dims=("time",),
                attrs={
                    "units": "days since 1970-1-1 0:0:0",
                    "long_name": "time",
                },
            ),
            VariableDescriptor(
                name="lon",
                dtype="float64",
                dims=("lon",),
                attrs={"units": "degrees_east", "long_name": "longitude"},
            ),
            VariableDescriptor(
                name="lat",
                dtype="float64",
                dims=("lat",),
                attrs={"units": "degrees_north", "long_name": "latitude"},
            ),
        ]
        return DatasetDescriptor(
            data_id=data_id,
            data_vars={desc.name: desc for desc in var_descriptors},
            coords={
                coord_desc.name: coord_desc for coord_desc in coord_descriptors
            },
            crs="WGS84",
            bbox=self._bbox,
            spatial_res=sensor_params.resolution,
            time_range=(sensor_params.start_date, sensor_params.end_date),
            time_period="10D",
            open_params_schema=self.get_open_data_params_schema(data_id),
        )

    def transform_params(
        self, opener_params: Dict, data_id: str
    ) -> Tuple[str, Dict[str, Any]]:
        self._validate_data_id(data_id)
        sensor = data_id.split(":")[1]
        sensor_params = _SENSOR_MAP[sensor]
        var_name = data_id.split(":")[2]

        time_params_from_range = self.transform_time_params(
            self.convert_time_range(opener_params["time_range"])
        )

        satellite = opener_params.get(
            "satellite", [sensor_params.satellites[-1]]
        )
        product_version = opener_params.get(
            "product_versions", [sensor_params.product_versions[-1]]
        )

        nominal_days = ["10", "20"]
        if "02" in time_params_from_range["month"]:
            for year in time_params_from_range["year"]:
                y = int(year)
                if (
                    "29" not in nominal_days
                    and (y % 4 == 0 and y % 100 != 0)
                    or (y % 400 == 0)
                ):
                    nominal_days.append("29")
                elif "28" not in nominal_days:
                    nominal_days.append("28")
        months_with_30_days = ["04", "06", "09", "11"]
        for m in months_with_30_days:
            if m in time_params_from_range["month"]:
                nominal_days.append("30")
                break
        months_with_31_days = ["01", "03", "05", "07", "08", "10", "12"]
        for m in months_with_31_days:
            if m in time_params_from_range["month"]:
                nominal_days.append("31")
                break

        cds_params = dict(
            variable=var_name,
            sensor=sensor,
            satellite=satellite,
            year=time_params_from_range["year"],
            month=time_params_from_range["month"],
            product_version=product_version,
            horizontal_resolution=[sensor_params.resolution],
            nominal_day=nominal_days,
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

        return _DATASET_NAME, unwrapped

    def read_file(
        self,
        dataset_name: str,
        open_params: Dict,
        cds_api_params: Dict[str, Union[str, List[str]]],
        file_path: str,
        temp_dir: str,
    ) -> xr.Dataset:
        return _read_file(file_path)
