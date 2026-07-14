# MIT License
#
# Copyright (c) 2020–2026 Brockmann Consult GmbH
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

"""Unit tests for Drought Indices dataset in the CDS Store

See test_store.py for further documentation.
"""

import datetime
import unittest
from test.mocks import get_cds_client

from xcube_cds.datasets.drought_indices_era5 import DroughtIndicesDatasetHandler
from xcube_cds.store import CDSDataOpener

_CDS_API_URL = "dummy"
_CDS_API_KEY = "dummy"


class CDSDroughtIndicesDatasetHandlerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.drought_idx_handler = DroughtIndicesDatasetHandler()
        self.data_id_reanalysis = "derived-drought-historical-monthly:reanalysis"
        self.data_id_ensemble = "derived-drought-historical-monthly:ensemble_members"

    def test_get_supported_data_ids(self):
        ids = self.drought_idx_handler.get_supported_data_ids()
        self.assertCountEqual([self.data_id_reanalysis, self.data_id_ensemble], ids)

    def test_get_human_readable_data_id(self):
        self.assertEqual(
            "Monthly drought indices from 1940–present derived "
            "from ERA5 reanalysis (main run)",
            self.drought_idx_handler.get_human_readable_data_id(
                self.data_id_reanalysis
            ),
        )

    def test_get_open_data_params_schema(self):
        schema = self.drought_idx_handler.get_open_data_params_schema()
        self.assertIn("variable_names", schema.properties)
        self.assertIn("accumulation_periods", schema.properties)
        self.assertIn("bbox", schema.properties)
        self.assertIn("time_range", schema.properties)
        self.assertNotIn("spatial_res", schema.properties)
        self.assertCountEqual(
            ["variable_names", "accumulation_periods", "bbox", "time_range"],
            schema.required,
        )

    def test_describe_data_reanalysis(self):
        descriptor = self.drought_idx_handler.describe_data(self.data_id_reanalysis)
        self.assertEqual(self.data_id_reanalysis, descriptor.data_id)
        self.assertEqual("EPSG:4326", descriptor.crs)
        self.assertEqual((-180.0, -90.0, 180.0, 90.0), descriptor.bbox)
        self.assertEqual(0.25, descriptor.spatial_res)
        self.assertEqual("1940-01-01", descriptor.time_range[0])
        self.assertEqual(
            (
                (datetime.datetime.now() - datetime.timedelta(days=90))
                .replace(day=1)
                .strftime("%Y-%m-%d")
            ),
            descriptor.time_range[1],
        )
        self.assertEqual("1M", descriptor.time_period)
        self.assertEqual(("time", "lat", "lon"), descriptor.data_vars["spi1"].dims)

    def test_describe_data_ensemble(self):
        descriptor = self.drought_idx_handler.describe_data(self.data_id_ensemble)
        self.assertEqual(self.data_id_ensemble, descriptor.data_id)
        self.assertEqual("EPSG:4326", descriptor.crs)
        self.assertEqual((-180.0, -90.0, 180.0, 90.0), descriptor.bbox)
        self.assertEqual(0.25, descriptor.spatial_res)
        self.assertEqual("1940-01-01", descriptor.time_range[0])
        self.assertEqual(
            (
                (datetime.datetime.now() - datetime.timedelta(days=90))
                .replace(day=1)
                .strftime("%Y-%m-%d")
            ),
            descriptor.time_range[1],
        )
        self.assertEqual("1M", descriptor.time_period)
        self.assertEqual(
            ("time", "realization", "lat", "lon"), descriptor.data_vars["spi1"].dims
        )

    def test_open_data_reanalysis(self):
        opener = CDSDataOpener(
            client_class=get_cds_client(),
            endpoint_url=_CDS_API_URL,
            cds_api_key=_CDS_API_KEY,
        )
        dataset = opener.open_data(
            self.data_id_reanalysis,
            variable_names=[
                "standardised_precipitation_index",
                "test_for_normality_spi",
            ],
            accumulation_periods=[1, 3],
            bbox=[-1, -1, 1, 1],
            time_range=["2015-10-15", "2016-02-02"],
        )
        self.assertIsNotNone(dataset)
        # Monthly data is timestamped at the first of the month, so we expect
        # four time co-ordinates (November to February inclusive).
        self.assertEqual(
            [4, 7, 7],
            [dataset.sizes["time"], dataset.sizes["lat"], dataset.sizes["lon"]],
        )
        self.assertCountEqual(
            ["spi1", "spi3", "spi1_significance", "spi3_significance"],
            dataset.data_vars,
        )

    def test_open_data_ensemble(self):
        opener = CDSDataOpener(
            client_class=get_cds_client(),
            endpoint_url=_CDS_API_URL,
            cds_api_key=_CDS_API_KEY,
        )
        dataset = opener.open_data(
            self.data_id_ensemble,
            variable_names=[
                "standardised_precipitation_index",
                "test_for_normality_spi",
            ],
            accumulation_periods=[1, 3],
            bbox=[-1, -1, 1, 1],
            time_range=["2015-10-15", "2016-02-02"],
        )
        self.assertIsNotNone(dataset)
        # Monthly data is timestamped at the first of the month, so we expect
        # four time co-ordinates (November to February inclusive).
        self.assertEqual(
            [4, 10, 7, 7],
            [
                dataset.sizes["time"],
                dataset.sizes["realization"],
                dataset.sizes["lat"],
                dataset.sizes["lon"],
            ],
        )
        self.assertCountEqual(
            ["spi1", "spi3", "spi1_significance", "spi3_significance"],
            dataset.data_vars,
        )

    def test_get_filepath_pattern_raises(self):
        with self.assertRaises(ValueError) as cm:
            self.drought_idx_handler._get_filepath_pattern("not_a_valid_key", 12)

        self.assertIn("Unknown var_name: not_a_valid_key", str(cm.exception))

    def test_get_varname_raises(self):
        with self.assertRaises(ValueError) as cm:
            self.drought_idx_handler._get_varname("not_a_valid_key", 12)

        self.assertIn("Unknown var_name: not_a_valid_key", str(cm.exception))
