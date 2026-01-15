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

"""Unit tests for Land Cover dataset in the CDS Store

See test_store.py for further documentation.
"""

import unittest
from test.mocks import get_cds_client

from xcube_cds.datasets.land_cover import LandCoverDatasetHandler
from xcube_cds.store import CDSDataOpener

_CDS_API_URL = "dummy"
_CDS_API_KEY = "dummy"


class CDSLandCoverHandlerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.land_cover_handler = LandCoverDatasetHandler()
        self.data_id = "satellite-land-cover"

    def test_get_supported_data_ids(self):
        ids = self.land_cover_handler.get_supported_data_ids()
        self.assertCountEqual([self.data_id], ids)

    def test_get_human_readable_data_id(self):
        self.assertEqual(
            "Land cover classification gridded maps from 1992 to present "
            "derived from satellite observations",
            self.land_cover_handler.get_human_readable_data_id(self.data_id),
        )

    def test_get_open_data_params_schema(self):
        schema = self.land_cover_handler.get_open_data_params_schema()
        self.assertIn("bbox", schema.properties)
        self.assertIn("time_range", schema.properties)
        self.assertNotIn("variable_names", schema.properties)
        self.assertNotIn("spatial_res", schema.properties)
        self.assertCountEqual(["time_range"], schema.required)

    def test_describe_data(self):
        descriptor = self.land_cover_handler.describe_data(self.data_id)
        self.assertEqual(self.data_id, descriptor.data_id)
        self.assertEqual("EPSG:4326", descriptor.crs)
        self.assertEqual((-180.0, -90.0, 180.0, 90.0), descriptor.bbox)
        self.assertEqual(0.00277778, descriptor.spatial_res)
        self.assertEqual("1992-01-01", descriptor.time_range[0])
        self.assertEqual("2022-12-31", descriptor.time_range[1])
        self.assertEqual("1Y", descriptor.time_period)
        self.assertEqual(
            ("time", "lat", "lon"), descriptor.data_vars["lccs_class"].dims
        )

    def test_open_data(self):
        opener = CDSDataOpener(
            client_class=get_cds_client(),
            endpoint_url=_CDS_API_URL,
            cds_api_key=_CDS_API_KEY,
        )
        dataset = opener.open_data(
            self.data_id,
            bbox=[-1, -1, 1, 1],
            time_range=["2015-01-01", "2016-12-31"],
        )
        self.assertIsNotNone(dataset)
        self.assertEqual(
            [2, 720, 720],
            [dataset.sizes["time"], dataset.sizes["lat"], dataset.sizes["lon"]],
        )
        self.assertCountEqual(
            [
                "lccs_class",
                "processed_flag",
                "current_pixel_state",
                "observation_count",
                "change_count",
                "lat_bounds",
                "lon_bounds",
                "time_bounds",
                "crs",
            ],
            dataset.data_vars,
        )
