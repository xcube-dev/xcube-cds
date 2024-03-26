# MIT License
#
# Copyright (c) 2020-2021 Brockmann Consult GmbH
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

""" Unit tests for ERA5 dataset in the CDS Store

See test_store.py for further documentation.
"""

import unittest

from jsonschema import ValidationError

import xcube
import xcube.core
from test.mocks import get_cds_client
from xcube.core.store import DATASET_TYPE
from xcube.core.store import VariableDescriptor
from xcube_cds.store import CDSDataOpener
from xcube_cds.store import CDSDataStore

_CDS_API_URL = "dummy"
_CDS_API_KEY = "dummy"


class CDSEra5Test(unittest.TestCase):
    def test_open(self):
        opener = CDSDataOpener(
            client_class=get_cds_client(),
            endpoint_url=_CDS_API_URL,
            cds_api_key=_CDS_API_KEY,
        )
        dataset = opener.open_data(
            "reanalysis-era5-single-levels-monthly-means:"
            "monthly_averaged_reanalysis",
            variable_names=["2m_temperature"],
            bbox=[-1, -1, 1, 1],
            spatial_res=0.25,
            time_range=["2015-10-15", "2016-02-02"],
        )
        self.assertIsNotNone(dataset)
        # We expect the closest representable time selection corresponding
        # to the requested range: months 10-12 and 1-2 for years 2015 and 2016,
        # thus (3 + 2) * 2 = 10 time-points in total.
        self.assertEqual(10, len(dataset.variables["time"]))

    def test_normalize_variable_names(self):
        store = CDSDataStore(
            client_class=get_cds_client(),
            normalize_names=True,
            endpoint_url=_CDS_API_URL,
            cds_api_key=_CDS_API_KEY,
        )
        dataset = store.open_data(
            "reanalysis-era5-single-levels-monthly-means:"
            "monthly_averaged_reanalysis",
            # Should be returned as p54.162, and normalized to p54_162.
            variable_names=["vertical_integral_of_temperature"],
            bbox=[-2, -2, 2, 2],
            spatial_res=1.0,
            time_range=["2019-01-01", "2020-12-31"],
        )
        self.assertIsNotNone(dataset)
        self.assertTrue("p54_162" in dataset.variables)

    def test_request_parameter_out_of_range(self):
        store = CDSDataStore(
            endpoint_url=_CDS_API_URL, cds_api_key=_CDS_API_KEY
        )
        with self.assertRaises(ValidationError):
            store.open_data(
                "reanalysis-era5-single-levels:ensemble_mean",
                variable_names=["2m_temperature"],
                bbox=[-1, -1, 181, 1],
                spatial_res=0.25,
                time_range=["2019-01-01", "2020-12-31"],
            )

    def test_era5_land_monthly(self):
        store = CDSDataStore(
            client_class=get_cds_client(),
            endpoint_url=_CDS_API_URL,
            cds_api_key=_CDS_API_KEY,
        )
        dataset = store.open_data(
            "reanalysis-era5-land-monthly-means:" "monthly_averaged_reanalysis",
            variable_names=["2m_temperature", "10m_u_component_of_wind"],
            bbox=[9.5, 49.5, 10.5, 50.5],
            spatial_res=0.1,
            time_range=["2015-01-01", "2016-12-31"],
        )
        self.assertIsNotNone(dataset)
        self.assertTrue("t2m" in dataset.variables)
        self.assertTrue("u10" in dataset.variables)

    def test_era5_single_levels_hourly(self):
        store = CDSDataStore(
            client_class=get_cds_client(),
            endpoint_url=_CDS_API_URL,
            cds_api_key=_CDS_API_KEY,
        )
        dataset = store.open_data(
            "reanalysis-era5-single-levels:" "reanalysis",
            variable_names=["2m_temperature"],
            bbox=[9, 49, 11, 51],
            spatial_res=0.25,
            time_range=["2015-01-01", "2015-01-02"],
        )
        self.assertIsNotNone(dataset)
        self.assertTrue("t2m" in dataset.variables)
        self.assertEqual(48, len(dataset.variables["time"]))

    def test_era5_land_hourly(self):
        store = CDSDataStore(
            client_class=get_cds_client(),
            endpoint_url=_CDS_API_URL,
            cds_api_key=_CDS_API_KEY,
        )
        dataset = store.open_data(
            "reanalysis-era5-land",
            variable_names=["2m_temperature"],
            bbox=[9.5, 49.5, 10.5, 50.5],
            spatial_res=0.1,
            time_range=["2015-01-01", "2015-01-02"],
        )
        self.assertIsNotNone(dataset)
        self.assertTrue("t2m" in dataset.variables)
        self.assertEqual(48, len(dataset.variables["time"]))

    def test_era5_bounds(self):
        opener = CDSDataOpener(
            client_class=get_cds_client(),
            endpoint_url=_CDS_API_URL,
            cds_api_key=_CDS_API_KEY,
        )
        dataset = opener.open_data(
            "reanalysis-era5-single-levels-monthly-means:"
            "monthly_averaged_reanalysis",
            variable_names=["2m_temperature"],
            bbox=[-180, -90, 180, 90],
            spatial_res=0.25,
            time_range=["2015-10-15", "2015-10-15"],
        )

        self.assertIsNotNone(dataset)

        west, south, east, north = xcube.core.geom.get_dataset_bounds(dataset)
        self.assertGreaterEqual(west, -180.0)
        self.assertGreaterEqual(south, -90.0)
        self.assertLessEqual(east, 180.0)
        self.assertLessEqual(north, 90.0)
        self.assertNotEqual(west, east)
        self.assertLessEqual(south, north)

    def test_era5_open_data_empty_variables_list(self):
        store = CDSDataStore(
            endpoint_url=_CDS_API_URL, cds_api_key=_CDS_API_KEY
        )
        dataset = store.open_data(
            "reanalysis-era5-land-monthly-means:monthly_averaged_reanalysis",
            variable_names=[],
            bbox=[-45, 0, 45, 60],
            spatial_res=0.25,
            time_range=["2015-01-01", "2016-12-31"],
        )
        self.assertEqual(len(dataset.data_vars), 0)
        self.assertEqual(24, len(dataset.variables["time"]))
        self.assertEqual(361, len(dataset.variables["lon"]))

    def test_open_data_null_variables_list(self):
        store = CDSDataStore(
            client_class=get_cds_client(),
            endpoint_url=_CDS_API_URL,
            cds_api_key=_CDS_API_KEY,
        )
        data_id = (
            "reanalysis-era5-single-levels-monthly-means:"
            "monthly_averaged_reanalysis"
        )
        schema = store.get_open_data_params_schema(data_id)
        n_vars = len(schema.properties["variable_names"].items.enum)
        dataset = store.open_data(
            data_id,
            variable_names=None,
            bbox=[-1, -1, 1, 1],
            spatial_res=0.25,
            time_range=["2015-10-15", "2015-10-15"],
        )
        self.assertEqual(n_vars, len(dataset.data_vars))

    def test_era5_describe_data(self):
        store = CDSDataStore(
            endpoint_url=_CDS_API_URL, cds_api_key=_CDS_API_KEY
        )
        descriptor = store.describe_data(
            "reanalysis-era5-single-levels:reanalysis"
        )
        self.assertEqual(265, len(descriptor.data_vars))
        self.assertEqual("WGS84", descriptor.crs)
        self.assertTupleEqual((-180, -90, 180, 90), descriptor.bbox)
        # We don't exhaustively check all 260 variables, but we check one
        # fully and make sure that the rest have correct type and dimensions.
        expected_vd = VariableDescriptor(
            name="u100",
            dtype="float32",
            dims=("time", "latitude", "longitude"),
            attrs=dict(units="m s**-1", long_name="100 metre U wind component"),
        )
        self.assertDictEqual(
            expected_vd.__dict__, descriptor.data_vars["u100"].__dict__
        )
        for vd in descriptor.data_vars.values():
            self.assertEqual("float32", vd.dtype)
            self.assertTupleEqual(("time", "latitude", "longitude"), vd.dims)

    def test_get_data_types_for_data(self):
        store = CDSDataStore()
        self.assertEqual(
            (DATASET_TYPE.alias,),
            store.get_data_types_for_data("reanalysis-era5-land"),
        )

    def test_has_data_true(self):
        self.assertTrue(CDSDataStore().has_data("reanalysis-era5-land"))
