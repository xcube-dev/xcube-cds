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

"""Unit tests for the xcube CDS store

Most of the tests use a mocked CDS API client which matches exact requests
and returns a saved result file originally downloaded from the real CDS API.
To create a new unit test of this kind,

1. Write a test which uses the real CDS API.
2. Temporarily add the _save_request_to and _save_file_to arguments to
   the open_data call.
3. Create a new subdirectory of test/mock_results, and move the saved request
   and results into it (as request.json and result, respectively). The name
   of the subdirectory is arbitrary, but it is useful to give it the same
   name as the unit test method.
4. Remove the _save_request_to and _save_file_to arguments from the open_data
   call, and add a 'client=CDSClientMock' argument to the CDSDataOpener
   constructor.
"""

import os
import tempfile
import unittest
from collections.abc import Iterator

import xcube
import xcube.core
from jsonschema import ValidationError
from xcube.core.store import TYPE_ID_DATASET
from xcube.core.store import VariableDescriptor, DataStoreError, DataDescriptor

from test.mocks import CDSClientMock
from xcube_cds.constants import CDS_DATA_OPENER_ID
from xcube_cds.store import CDSDataOpener
from xcube_cds.store import CDSDataStore
from xcube_cds.store import CDSDatasetHandler


class CDSStoreTest(unittest.TestCase):

    def test_open(self):
        opener = CDSDataOpener(client=CDSClientMock)
        dataset = opener.open_data(
            'reanalysis-era5-single-levels-monthly-means:'
            'monthly_averaged_reanalysis',
            variable_names=['2m_temperature'],
            bbox=[-1, -1, 1, 1],
            spatial_res=0.25,
            time_period='1M',
            time_range=['2015-10-15', '2016-02-02'],
        )
        self.assertIsNotNone(dataset)
        # We expect the closest representable time selection corresponding
        # to the requested range: months 10-12 and 1-2 for years 2015 and 2016,
        # thus (3 + 2) * 2 = 10 time-points in total.
        self.assertEqual(10, len(dataset.variables['time']))

    def test_normalize_variable_names(self):
        store = CDSDataStore(client=CDSClientMock, normalize_names=True)
        dataset = store.open_data(
            'reanalysis-era5-single-levels-monthly-means:'
            'monthly_averaged_reanalysis',
            # Should be returned as p54.162, and normalized to p54_162.
            variable_names=['vertical_integral_of_temperature'],
            bbox=[-2, -2, 2, 2],
            spatial_res=1.0,
            time_range=['2019-01-01', None],
        )
        self.assertIsNotNone(dataset)
        self.assertTrue('p54_162' in dataset.variables)

    def test_invalid_data_id(self):
        store = CDSDataStore()
        with self.assertRaises(ValueError):
            store.open_data(
                'this-data-id-does-not-exist',
                variable_names=['2m_temperature'],
                hours=[0], months=[1], years=[2019]
            )

    def test_request_parameter_out_of_range(self):
        store = CDSDataStore()
        with self.assertRaises(ValidationError):
            store.open_data(
                'reanalysis-era5-single-levels-monthly-means:'
                'monthly_averaged_reanalysis_by_hour_of_day',
                variable_names=['2m_temperature'],
                bbox=[-1, -1, 181, 1],
                spatial_res=0.25,
                time_period='1M',
                time_range=['2019-01-01', '2020-12-31']
            )

    def test_era5_land_monthly(self):
        store = CDSDataStore(client=CDSClientMock)
        dataset = store.open_data(
            'reanalysis-era5-land-monthly-means:'
            'monthly_averaged_reanalysis',
            variable_names=['2m_temperature', '10m_u_component_of_wind'],
            bbox=[9.5, 49.5, 10.5, 50.5],
            spatial_res=0.1,
            time_period='1M',
            time_range=['2015-01-01', '2016-12-31'],
        )
        self.assertIsNotNone(dataset)
        self.assertTrue('t2m' in dataset.variables)
        self.assertTrue('u10' in dataset.variables)

    def test_era5_single_levels_hourly(self):
        store = CDSDataStore(client=CDSClientMock)
        dataset = store.open_data(
            'reanalysis-era5-single-levels:'
            'reanalysis',
            variable_names=['2m_temperature'],
            bbox=[9, 49, 11, 51],
            spatial_res=0.25,
            time_period='1H',
            time_range=['2015-01-01 20:00',
                        '2015-01-02 08:00'],
        )
        self.assertIsNotNone(dataset)
        self.assertTrue('t2m' in dataset.variables)
        self.assertEqual(26, len(dataset.variables['time']))

    def test_era5_land_hourly(self):
        store = CDSDataStore(client=CDSClientMock)
        dataset = store.open_data(
            'reanalysis-era5-land',
            variable_names=['2m_temperature'],
            bbox=[9.5, 49.5, 10.5, 50.5],
            spatial_res=0.1,
            time_period='1H',
            time_range=['2015-01-01 20:00',
                        '2015-01-02 08:00'],
        )
        self.assertIsNotNone(dataset)
        self.assertTrue('t2m' in dataset.variables)
        self.assertEqual(26, len(dataset.variables['time']))

    def test_era5_bounds(self):
        opener = CDSDataOpener(client=CDSClientMock)
        dataset = opener.open_data(
            'reanalysis-era5-single-levels-monthly-means:'
            'monthly_averaged_reanalysis',
            variable_names=['2m_temperature'],
            bbox=[-180, -90, 180, 90],
            spatial_res=0.25,
            time_period='1M',
            time_range=['2015-10-15', '2015-10-15'],
        )

        self.assertIsNotNone(dataset)

        west, south, east, north = xcube.core.geom.get_dataset_bounds(dataset)
        self.assertGreaterEqual(west, -180.0)
        self.assertGreaterEqual(south, -90.0)
        self.assertLessEqual(east, 180.0)
        self.assertLessEqual(north, 90.0)
        self.assertNotEqual(west, east)
        self.assertLessEqual(south, north)

    def test_soil_moisture(self):
        store = CDSDataStore(client=CDSClientMock)
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

    def test_list_and_describe_data_ids(self):
        store = CDSDataStore()
        data_ids = store.get_data_ids()
        self.assertIsInstance(data_ids, Iterator)
        for data_id in data_ids:
            self.assertIsInstance(data_id, tuple)
            self.assertTrue(1 <= len(data_id) <= 2)
            for element in data_id:
                self.assertIsInstance(element, str)
            descriptor = store.describe_data(data_id[0])
            self.assertIsInstance(descriptor, DataDescriptor)

    def test_open_data_no_variables_1(self):
        store = CDSDataStore()
        dataset = store.open_data(
            'satellite-soil-moisture:volumetric:monthly',
            variable_names=[],
            bbox=[-45, 0, 45, 60],
            spatial_res=0.25,
            time_period='1M',
            time_range=['2015-01-01', '2016-12-31']
        )
        self.assertEqual(len(dataset.data_vars), 0)
        self.assertEqual(24, len(dataset.variables['time']))
        self.assertEqual(361, len(dataset.variables['lon']))

    def test_open_data_no_variables_2(self):
        store = CDSDataStore()
        dataset = store.open_data(
            'satellite-soil-moisture:volumetric:10-day',
            variable_names=[],
            bbox=[10.1, -14, 12.9, -4],
            spatial_res=0.25,
            time_period='10D',
            time_range=['1981-06-14T11:39:21.666',
                        '1982-02-13T09:32:34.321']
        )
        self.assertEqual(len(dataset.data_vars), 0)
        self.assertEqual(26, len(dataset.variables['time']))
        self.assertEqual(13, len(dataset.variables['lon']))

    def test_era5_describe_data(self):
        store = CDSDataStore()
        descriptor = store.describe_data(
            'reanalysis-era5-single-levels-monthly-means:'
            'monthly_averaged_ensemble_members_by_hour_of_day')
        self.assertEqual(260, len(descriptor.data_vars))
        self.assertEqual('WGS84', descriptor.crs)
        self.assertTupleEqual((-180, -90, 180, 90), descriptor.bbox)
        # We don't exhaustively check all 260 variables, but we check the
        # first one and make sure that they all have correct type and
        # dimensions.
        expected_vd = VariableDescriptor(
            name='u100',
            dtype='float32',
            dims=('time', 'latitude', 'longitude'),
            attrs=dict(units='m s**-1', long_name='100 metre U wind component'))
        self.assertDictEqual(expected_vd.__dict__,
                             descriptor.data_vars[0].__dict__)
        for vd in descriptor.data_vars:
            self.assertEqual('float32', vd.dtype)
            self.assertTupleEqual(('time', 'latitude', 'longitude'),
                                  vd.dims)

    def test_convert_invalid_time_range(self):
        with self.assertRaises(ValueError):
            CDSDatasetHandler.convert_time_range([])  # incorrect list length

    def test_get_open_params_schema_without_data_id(self):
        opener = CDSDataOpener()
        schema = opener.get_open_data_params_schema()

        actual = schema.to_dict()
        self.assertCountEqual(['type', 'properties', 'required'], actual.keys())
        self.assertEqual('object', actual['type'])
        self.assertCountEqual(
            ['bbox', 'spatial_res', 'variable_names', 'time_range'],
            actual['required'])
        self.assertCountEqual(
            ['dataset_name', 'variable_names', 'crs', 'bbox', 'spatial_res',
             'time_range', 'time_period'],
            actual['properties'].keys()
        )

    def test_search_data_invalid_id(self):
        store = CDSDataStore()
        with self.assertRaises(DataStoreError):
            store.search_data('This is an invalid ID.')

    def test_search_data_valid_id(self):
        store = CDSDataStore()
        # The CDS API doesn't offer a search function, so "not implemented"
        # is expected here.
        with self.assertRaises(NotImplementedError):
            store.search_data('dataset')

    def test_copy_on_open(self):
        store = CDSDataStore(client=CDSClientMock)
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

    def test_get_data_store_params_schema(self):
        self.assertDictEqual({
            'type': 'object',
            'properties': {
                'normalize_names': {'type': 'boolean', 'default': False},
                'num_retries': {'type': 'integer', 'default': 200,
                                'minimum': 0}},
            'additionalProperties': False
        }, CDSDataStore.get_data_store_params_schema().to_dict())

    def test_get_type_ids(self):
        self.assertTupleEqual((TYPE_ID_DATASET, ), CDSDataStore.get_type_ids())

    def test_has_data_true(self):
        self.assertTrue(CDSDataStore().has_data('reanalysis-era5-land'))

    def test_has_data_false(self):
        self.assertFalse(CDSDataStore().has_data('nonexistent data ID'))

    def test_get_data_opener_ids_invalid_type_id(self):
        with self.assertRaises(DataStoreError):
            CDSDataStore().get_data_opener_ids(CDS_DATA_OPENER_ID,
                                               'this is an invalid ID')

    def test_get_data_opener_ids_invalid_opener_id(self):
        with self.assertRaises(DataStoreError):
            CDSDataStore().get_data_opener_ids('this is an invalid ID',
                                               TYPE_ID_DATASET)

    def test_get_data_opener_ids_with_default_arguments(self):
        self.assertTupleEqual((CDS_DATA_OPENER_ID, ),
                              CDSDataStore().get_data_opener_ids())


if __name__ == '__main__':
    unittest.main()
