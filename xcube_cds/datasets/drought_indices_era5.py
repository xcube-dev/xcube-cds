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

import pandas as pd
import xarray as xr
from xcube.core.store import DatasetDescriptor, VariableDescriptor
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonDateSchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
import zipfile

from xcube_cds.store import CDSDatasetHandler


class DroughtIndicesDatasetHandler(CDSDatasetHandler):

    def __init__(self):
        self._data_id_map = {
            "derived-drought-historical-monthly:reanalysis": (
                "Monthly drought indices from 1940–present derived "
                "from ERA5 reanalysis (main run)"
            ),
        }
        self._variable_names = [
            "standardised_precipitation_index",
            "standardised_precipitation_evapotranspiration_index",
            "probability_of_zero_precipitation_spi",
            "test_for_normality_spi",
            "test_for_normality_spei",
        ]
        self._accumulation_periods = [1, 3, 6, 12, 24, 36, 48]
        self._min_date = "1940-01-01"
        self._max_date = "2025-12-31"
        self._bbox = (-180.0, -90.0, 180.0, 90.0)
        self._spatial_res = 0.25

    def get_supported_data_ids(self) -> list[str]:
        return list(self._data_id_map)

    def get_open_data_params_schema(
        self, data_id: str | None = None
    ) -> JsonObjectSchema:
        params = dict(
            variable_names=JsonArraySchema(
                items=(JsonStringSchema(enum=self._variable_names)),
                min_items=1,
                unique_items=True,
                nullable=True,
                description="identifiers of the requested variables",
            ),
            accumulation_periods=JsonArraySchema(
                items=(JsonNumberSchema(enum=self._accumulation_periods)),
                unique_items=True,
                nullable=True,
                description="Accumulation windows in months",
            ),
            bbox=JsonArraySchema(
                items=(
                    JsonNumberSchema(minimum=self._bbox[0], maximum=self._bbox[2]),
                    JsonNumberSchema(minimum=self._bbox[1], maximum=self._bbox[3]),
                    JsonNumberSchema(minimum=self._bbox[0], maximum=self._bbox[2]),
                    JsonNumberSchema(minimum=self._bbox[1], maximum=self._bbox[3]),
                ),
                description="bounding box (min_x, min_y, max_x, max_y)",
            ),
            time_range=JsonDateSchema.new_range(
                min_date=self._min_date, max_date=self._max_date
            ),
        )
        required = [
            "variable_names",
            "accumulation_periods",
            "bbox",
            "time_range",
        ]
        return JsonObjectSchema(
            properties=params, required=required, additional_properties=False
        )

    def get_human_readable_data_id(self, data_id: str) -> str:
        return self._data_id_map[data_id]

    def describe_data(self, data_id: str) -> DatasetDescriptor:
        mapping_varname_attrs = dict()
        for var_name in self._variable_names:
            for accum_period in self._accumulation_periods:
                ds_varname = self._get_varname(var_name, accum_period)
                mapping_varname_attrs[ds_varname] = self._get_attrs(
                    var_name, accum_period
                )

        variable_descriptors = []
        for var_name, attrs in mapping_varname_attrs.items():
            variable_descriptors.append(
                VariableDescriptor(
                    name=var_name,
                    dtype="float64",
                    dims=("time", "lat", "lon"),
                    attrs=attrs,
                )
            )
        return DatasetDescriptor(
            data_id=data_id,
            data_vars={desc.name: desc for desc in variable_descriptors},
            crs="EPSG:4326",
            bbox=self._bbox,
            spatial_res=self._spatial_res,
            time_range=(self._min_date, self._max_date),
            time_period="1M",
            open_params_schema=self.get_open_data_params_schema(data_id),
        )

    def transform_params(self, plugin_params: dict, data_id: str) -> tuple[str, dict]:
        dataset_name, product_type = data_id.split(":")

        # Convert the time range specification to the nearest equivalent
        # in the CDS "orthogonal time units" scheme.
        time_params_from_range = self.transform_time_params(
            self.convert_time_range(plugin_params["time_range"])
        )

        params_combined = {
            "variable": plugin_params["variable_names"],
            "accumulation_period": [
                str(i) for i in plugin_params["accumulation_periods"]
            ],
            "area": [
                plugin_params["bbox"][3] - self._spatial_res / 2,
                plugin_params["bbox"][0] + self._spatial_res / 2,
                plugin_params["bbox"][1] + self._spatial_res / 2,
                plugin_params["bbox"][2] - self._spatial_res / 2,
            ],
            "version": "1_0",
            "product_type": [product_type],
            "dataset_type": "consolidated_dataset",
            "year": time_params_from_range["year"],
            "month": time_params_from_range["month"],
        }

        return dataset_name, params_combined

    def read_file(
        self,
        dataset_name: str,
        open_params: dict,
        cds_api_params: dict,
        file_path: str,
        temp_dir: str,
    ) -> xr.Dataset:
        path_temp = os.path.join(pathlib.Path(file_path).parent.resolve(), "temp")
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(path_temp)
        file_paths = glob.glob(f"{path_temp}/*")
        dss = []
        for var_name in open_params["variable_names"]:
            for accum_period in open_params["accumulation_periods"]:
                pattern = self._get_filepath_pattern(var_name, accum_period)
                file_sel = [path for path in file_paths if pattern in path]
                file_sel = sorted(file_sel)
                ds = xr.open_mfdataset(
                    file_sel,
                    engine="netcdf4",
                    chunks="auto",
                    combine_attrs="drop_conflicts",
                )
                if "standardised_precipitation" in var_name:
                    ds = ds.sel(
                        time=slice(
                            open_params["time_range"][0], open_params["time_range"][1]
                        )
                    )
                else:
                    ds = self._resample_quality_ds(ds, open_params["time_range"])
                assert len(ds.data_vars) == 1
                ds_varname = self._get_varname(var_name, accum_period)
                ds = ds.rename({list(ds.data_vars.keys())[0]: ds_varname})
                dss.append(ds)
        ds_final = xr.merge(dss, join="outer", combine_attrs="drop_conflicts")
        ds_final = ds_final.sel(
            time=slice(open_params["time_range"][0], open_params["time_range"][1])
        )
        return ds_final

    @staticmethod
    def _get_filepath_pattern(var_name: str, accum_period: int) -> str:
        patterns = {
            "standardised_precipitation_index": f"SPI{accum_period}_gamma_global",
            "standardised_precipitation_evapotranspiration_index": f"SPEI{accum_period}_genlogistic_global",
            "probability_of_zero_precipitation_spi": f"SPI{accum_period}_spipzero_gamma_global",
            "test_for_normality_spi": f"SPI{accum_period}_spisignificance_gamma",
            "test_for_normality_spei": f"SPEI{accum_period}_speisignificance_genlogistic_global",
        }
        try:
            return patterns[var_name]
        except KeyError:
            raise ValueError(f"Unknown var_name: {var_name}")

    @staticmethod
    def _get_attrs(var_name: str, accum_period: int) -> dict:
        mapping_attrs = {
            "standardised_precipitation_index": {
                "long_name": f"Standardized Drought Index (SPI{accum_period})"
            },
            "standardised_precipitation_evapotranspiration_index": {
                "long_name": f"Standardized Drought Index (SPEI{accum_period})"
            },
            "probability_of_zero_precipitation_spi": {
                "long_name": (
                    "Number of months with zero precipitation during the reference "
                    "period expressed as the fraction of the total number of months"
                ),
                "unit": 1,
            },
            "test_for_normality_spi": {
                "long_name": (
                    "Quality flag that indicates the acceptance of a Shapiro-Wilks "
                    "test for normality using the derived standardized indices during "
                    "the reference period; 1 indicates significant values and 0 "
                    "indicates non-significant values at the alpha=0.05 level."
                ),
                "unit": 1,
            },
            "test_for_normality_spei": {
                "long_name": (
                    "Quality flag that indicates the acceptance of a Shapiro-Wilks "
                    "test for normality using the derived standardized indices during "
                    "the reference period; 1 indicates significant values and 0 "
                    "indicates non-significant values at the alpha=0.05 level."
                ),
                "unit": 1,
            },
        }
        return mapping_attrs[var_name]

    @staticmethod
    def _get_varname(var_name: str, accum_period: int) -> str:
        ds_varnames = {
            "standardised_precipitation_index": f"spi{accum_period}",
            "standardised_precipitation_evapotranspiration_index": f"spei{accum_period}",
            "probability_of_zero_precipitation_spi": f"spi{accum_period}_pzero",
            "test_for_normality_spi": f"spi{accum_period}_significance",
            "test_for_normality_spei": f"spei{accum_period}_significance",
        }
        try:
            return ds_varnames[var_name]
        except KeyError:
            raise ValueError(f"Unknown var_name: {var_name}")

    @staticmethod
    def _resample_quality_ds(ds: xr.Dataset, time_range: tuple[str, str]) -> xr.Dataset:
        time_target = xr.date_range(
            start=time_range[0], end=time_range[1], freq="MS"
        ) + pd.Timedelta(hours=6)
        target_months = time_target.to_period("M").month
        ds = ds.assign_coords(month=ds["time"].dt.month).swap_dims({"time": "month"})
        ds_resampled = ds.sel(month=target_months)
        ds_resampled = ds_resampled.drop_vars("time")
        ds_resampled = ds_resampled.rename({"month": "time"})
        ds_resampled = ds_resampled.drop_vars("time")
        ds_resampled = ds_resampled.assign_coords(
            time=time_target.values.astype("datetime64[ns]")
        )
        return ds_resampled
