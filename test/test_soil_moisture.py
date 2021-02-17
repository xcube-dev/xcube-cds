# MIT License
#
# Copyright (c) 2020 Brockmann Consult GmbH
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

import os
import tempfile
import unittest

from test.mocks import CDSClientMock
from xcube_cds.store import CDSDataStore

_CDS_API_URL = 'dummy'
_CDS_API_KEY = 'dummy'


class CDSSoilMoistureTest(unittest.TestCase):

    def test_soil_moisture(self):
        store = CDSDataStore(client_class=CDSClientMock,
                             cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        data_id = 'satellite-soil-moisture:volumetric:monthly'
        dataset = store.open_data(
            data_id,
            variable_names=['volumetric_surface_soil_moisture'],
            bbox=[-180, -90, 180, 90],
            spatial_res=0.25,
            time_period='1M',
            time_range=['2015-01-01', '2015-02-28'],
        )
        self.assertTrue('sm' in dataset.variables)
        self.assertEqual(2, len(dataset.variables['time']))
        self.assertEqual('2014-12-31T12:00:00Z',
                         dataset.attrs['time_coverage_start'])
        self.assertEqual('2015-02-28T12:00:00Z',
                         dataset.attrs['time_coverage_end'])
        description = store.describe_data(data_id)
        self.assertEqual(sorted([dv.name for dv in description.data_vars]),
                         sorted(map(str, dataset.data_vars)))

    def test_soil_moisture_without_optional_parameters(self):
        store = CDSDataStore(client_class=CDSClientMock,
                             cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        data_id = 'satellite-soil-moisture:volumetric:monthly'
        dataset = store.open_data(
            data_id,
            variable_names=['volumetric_surface_soil_moisture'],
            time_range=['2015-01-01', '2015-02-28'],
        )
        self.assertTrue('sm' in dataset.variables)
        self.assertEqual(2, len(dataset.variables['time']))
        self.assertEqual('2014-12-31T12:00:00Z',
                         dataset.attrs['time_coverage_start'])
        self.assertEqual('2015-02-28T12:00:00Z',
                         dataset.attrs['time_coverage_end'])
        description = store.describe_data(data_id)
        self.assertEqual(sorted([dv.name for dv in description.data_vars]),
                         sorted(map(str, dataset.data_vars)))

    def test_open_data_empty_variables_list_2(self):
        store = CDSDataStore(cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        dataset = store.open_data(
            'satellite-soil-moisture:volumetric:10-day',
            variable_names=[],
            bbox=[-180, -90, 180, 90],
            spatial_res=0.25,
            time_period='10D',
            time_range=['1981-06-14',
                        '1982-02-13']
        )
        self.assertEqual(len(dataset.data_vars), 0)
        self.assertEqual(26, len(dataset.variables['time']))
        self.assertEqual(1441, len(dataset.variables['lon']))

    def test_copy_on_open(self):
        store = CDSDataStore(client_class=CDSClientMock,
                             cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        data_id = 'satellite-soil-moisture:volumetric:monthly'
        with tempfile.TemporaryDirectory() as temp_dir:
            request_path = os.path.join(temp_dir, 'request.json')
            result_path = os.path.join(temp_dir, 'result')
            zarr_path = os.path.join(temp_dir, 'result.zarr')
            store.open_data(
                data_id,
                _save_request_to=request_path,
                _save_file_to=result_path,
                _save_zarr_to=zarr_path,
                variable_names=['volumetric_surface_soil_moisture'],
                bbox=[-180, -90, 180, 90],
                spatial_res=0.25,
                time_period='1M',
                time_range=['2015-01-01', '2015-02-28'],
            )
            self.assertTrue(os.path.isfile(request_path))
            self.assertTrue(os.path.isfile(result_path))
            self.assertTrue(os.path.isdir(zarr_path))
