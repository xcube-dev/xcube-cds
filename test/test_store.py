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
   call, and add a 'client_class=CDSClientMock' argument to the CDSDataOpener
   constructor.
"""

import os
import tempfile
import unittest
from collections.abc import Iterator

import xcube
import xcube.core
from jsonschema import ValidationError
from xcube.core.store import TYPE_SPECIFIER_DATASET, TYPE_SPECIFIER_CUBE
from xcube.core.store import VariableDescriptor, DataStoreError, DataDescriptor

from test.mocks import CDSClientMock
from xcube_cds.constants import CDS_DATA_OPENER_ID
from xcube_cds.store import CDSDataOpener
from xcube_cds.store import CDSDataStore
from xcube_cds.store import CDSDatasetHandler

_CDS_API_URL = 'dummy'
_CDS_API_KEY = 'dummy'


class CDSStoreTest(unittest.TestCase):

    def test_open(self):
        opener = CDSDataOpener(client_class=CDSClientMock,
                               cds_api_url=_CDS_API_URL,
                               cds_api_key=_CDS_API_KEY)
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
        store = CDSDataStore(client_class=CDSClientMock, normalize_names=True,
                             cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        dataset = store.open_data(
            'reanalysis-era5-single-levels-monthly-means:'
            'monthly_averaged_reanalysis',
            # Should be returned as p54.162, and normalized to p54_162.
            variable_names=['vertical_integral_of_temperature'],
            bbox=[-2, -2, 2, 2],
            spatial_res=1.0,
            time_range=['2019-01-01', '2020-12-31'],
        )
        self.assertIsNotNone(dataset)
        self.assertTrue('p54_162' in dataset.variables)

    def test_invalid_data_id(self):
        store = CDSDataStore(cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        with self.assertRaises(ValueError):
            store.open_data(
                'this-data-id-does-not-exist',
                variable_names=['2m_temperature'],
                hours=[0], months=[1], years=[2019]
            )

    def test_request_parameter_out_of_range(self):
        store = CDSDataStore(cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        with self.assertRaises(ValidationError):
            store.open_data(
                'reanalysis-era5-single-levels:ensemble_mean',
                variable_names=['2m_temperature'],
                bbox=[-1, -1, 181, 1],
                spatial_res=0.25,
                time_period='1M',
                time_range=['2019-01-01', '2020-12-31']
            )

    def test_era5_land_monthly(self):
        store = CDSDataStore(client_class=CDSClientMock,
                             cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
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
        store = CDSDataStore(client_class=CDSClientMock,
                             cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        dataset = store.open_data(
            'reanalysis-era5-single-levels:'
            'reanalysis',
            variable_names=['2m_temperature'],
            bbox=[9, 49, 11, 51],
            spatial_res=0.25,
            time_period='1H',
            time_range=['2015-01-01',
                        '2015-01-02'],
        )
        self.assertIsNotNone(dataset)
        self.assertTrue('t2m' in dataset.variables)
        self.assertEqual(48, len(dataset.variables['time']))

    def test_era5_land_hourly(self):
        store = CDSDataStore(client_class=CDSClientMock,
                             cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        dataset = store.open_data(
            'reanalysis-era5-land',
            variable_names=['2m_temperature'],
            bbox=[9.5, 49.5, 10.5, 50.5],
            spatial_res=0.1,
            time_period='1H',
            time_range=['2015-01-01',
                        '2015-01-02'],
        )
        self.assertIsNotNone(dataset)
        self.assertTrue('t2m' in dataset.variables)
        self.assertEqual(48, len(dataset.variables['time']))

    def test_era5_bounds(self):
        opener = CDSDataOpener(client_class=CDSClientMock,
                               cds_api_url=_CDS_API_URL,
                               cds_api_key=_CDS_API_KEY)
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

    def test_list_and_describe_data_ids(self):
        store = CDSDataStore(cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        data_ids = store.get_data_ids()
        self.assertIsInstance(data_ids, Iterator)
        for data_id in data_ids:
            self.assertIsInstance(data_id, tuple)
            self.assertTrue(1 <= len(data_id) <= 2)
            for element in data_id:
                self.assertIsInstance(element, str)
            descriptor = store.describe_data(data_id[0])
            self.assertIsInstance(descriptor, DataDescriptor)

    def test_open_data_empty_variables_list_1(self):
        store = CDSDataStore(cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        dataset = store.open_data(
            'reanalysis-era5-land-monthly-means:monthly_averaged_reanalysis',
            variable_names=[],
            bbox=[-45, 0, 45, 60],
            spatial_res=0.25,
            time_period='1M',
            time_range=['2015-01-01', '2016-12-31']
        )
        self.assertEqual(len(dataset.data_vars), 0)
        self.assertEqual(24, len(dataset.variables['time']))
        self.assertEqual(361, len(dataset.variables['lon']))

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

    def test_open_data_null_variables_list(self):
        store = CDSDataStore(client_class=CDSClientMock,
                             cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        data_id = 'reanalysis-era5-single-levels-monthly-means:'\
            'monthly_averaged_reanalysis'
        schema = store.get_open_data_params_schema(data_id)
        n_vars = len(schema.properties['variable_names'].items.enum)
        dataset = store.open_data(
            data_id,
            variable_names=None,
            bbox=[-1, -1, 1, 1],
            spatial_res=0.25,
            time_period='1M',
            time_range=['2015-10-15', '2015-10-15']
        )
        self.assertEqual(n_vars, len(dataset.data_vars))

    def test_era5_describe_data(self):
        store = CDSDataStore(cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        descriptor = store.describe_data(
            'reanalysis-era5-single-levels:reanalysis')
        self.assertEqual(265, len(descriptor.data_vars))
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
        opener = CDSDataOpener(cds_api_url=_CDS_API_URL,
                               cds_api_key=_CDS_API_KEY)
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
        store = CDSDataStore(cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        with self.assertRaises(DataStoreError):
            store.search_data('This is an invalid ID.')

    def test_search_data_valid_id(self):
        store = CDSDataStore(cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        # The CDS API doesn't offer a search function, so "not implemented"
        # is expected here.
        with self.assertRaises(NotImplementedError):
            store.search_data('dataset')

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

    def test_get_data_store_params_schema(self):
        self.assertDictEqual({
            'type': 'object',
            'properties': {
                'normalize_names': {'type': 'boolean', 'default': False},
                'num_retries': {'type': 'integer', 'default': 200,
                                'minimum': 0}},
            'additionalProperties': False
        }, CDSDataStore.get_data_store_params_schema().to_dict())

    def test_get_type_specifiers(self):
        self.assertTupleEqual((TYPE_SPECIFIER_CUBE, ),
                              CDSDataStore.get_type_specifiers())

    def test_get_type_specifiers_for_data(self):
        store = CDSDataStore()
        self.assertEqual(
            (TYPE_SPECIFIER_CUBE, ),
            store.get_type_specifiers_for_data('reanalysis-era5-land')
        )

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
                                               TYPE_SPECIFIER_DATASET)

    def test_get_data_opener_ids_with_default_arguments(self):
        self.assertTupleEqual((CDS_DATA_OPENER_ID, ),
                              CDSDataStore().get_data_opener_ids())

    def test_get_store_open_params_schema_without_data_id(self):
        self.assertIsInstance(
            CDSDataStore().get_open_data_params_schema(),
            xcube.util.jsonschema.JsonObjectSchema
        )

    def test_get_data_ids(self):
        store = CDSDataStore(client_class=CDSClientMock,
                             cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        self.assertEqual([], list(store.get_data_ids('unsupported_type_spec')))
        self.assertEqual([],
                         list(store.get_data_ids('dataset[unsupported_flag]')))

        # The number of available datasets is expected to increase over time,
        # so to avoid overfitting the test we just check that more than a few
        # datasets and/or cubes are available. "a few" is semi-arbitrarily
        # defined to be 5.
        minimum_expected_datasets = 5
        self.assertGreater(len(list(store.get_data_ids('dataset'))),
                           minimum_expected_datasets)
        self.assertGreater(len(list(store.get_data_ids('dataset[cube]'))),
                           minimum_expected_datasets)


class ClientUrlTest(unittest.TestCase):

    """Tests connected with passing CDS API URL and key to opener or store."""

    def setUp(self):
        self.old_environment = dict(os.environ)
        self.temp_dir = tempfile.TemporaryDirectory()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.old_environment)
        self.temp_dir.cleanup()

    def test_client_url_and_key_parameters(self):
        """Test passing URL and key parameters to client constructor

        This test verifies that the CDS API URL and key, when specified as
        parameters to CDSDataOpener, are correctly passed to the CDS client,
        overriding any configuration file or environment variable settings.
        """

        self._set_up_api_configuration('wrong URL 1', 'wrong key 1',
                                       'wrong URL 2', 'wrong key 2')
        cds_api_url = 'https://example.com/'
        cds_api_key = 'xyzzy'
        opener = CDSDataOpener(client_class=CDSClientMock,
                               cds_api_url=cds_api_url,
                               cds_api_key=cds_api_key)
        opener.open_data(
            'reanalysis-era5-single-levels-monthly-means:'
            'monthly_averaged_reanalysis',
            variable_names=['2m_temperature'],
            bbox=[-180, -90, 180, 90],
            spatial_res=0.25,
            time_period='1M',
            time_range=['2015-10-15', '2015-10-15'],
        )
        client = opener.last_instantiated_client
        self.assertEqual(cds_api_url, client.url)
        self.assertEqual(cds_api_key, client.key)

    def test_client_url_and_key_environment_variables(self):
        """Test passing URL and key parameters via environment variables

        This test verifies that the CDS API URL and key, when specified in
        environment variables, are correctly passed to the CDS client,
        overriding any configuration file settings.
        """

        cds_api_url = 'https://example.com/'
        cds_api_key = 'xyzzy'
        self._set_up_api_configuration('wrong URL 1', 'wrong key 1',
                                       cds_api_url, cds_api_key)
        client = self._get_client()
        self.assertEqual(cds_api_url, client.url)
        self.assertEqual(cds_api_key, client.key)

    def test_client_url_and_key_rc_file(self):
        """Test passing URL and key parameters via environment variables

        This test verifies that the CDS API URL and key, when specified in
        a configuration file, are correctly passed to the CDS client.
        """

        cds_api_url = 'https://example.com/'
        cds_api_key = 'xyzzy'
        self._set_up_api_configuration(cds_api_url, cds_api_key)
        opener = CDSDataOpener(client_class=CDSClientMock)
        opener.open_data(
            'reanalysis-era5-single-levels-monthly-means:'
            'monthly_averaged_reanalysis',
            variable_names=['2m_temperature'],
            bbox=[-180, -90, 180, 90],
            spatial_res=0.25,
            time_period='1M',
            time_range=['2015-10-15', '2015-10-15'],
        )
        client = opener.last_instantiated_client
        self.assertEqual(cds_api_url, client.url)
        self.assertEqual(cds_api_key, client.key)

    @staticmethod
    def _get_client(**opener_args):
        """Return the client instantiated to open a dataset

        Open a dataset and return the client that was instantiated to execute
        the CDS API query.
        """
        opener = CDSDataOpener(client_class=CDSClientMock, **opener_args)
        opener.open_data(
            'reanalysis-era5-single-levels-monthly-means:'
            'monthly_averaged_reanalysis',
            variable_names=['2m_temperature'],
            bbox=[-180, -90, 180, 90],
            spatial_res=0.25,
            time_period='1M',
            time_range=['2015-10-15', '2015-10-15'],
        )
        return opener.last_instantiated_client

    def _set_up_api_configuration(self, url_rc, key_rc,
                                  url_env=None, key_env=None):
        """Set up a configuration file and, optionally, environment variables

        The teardown function will take care of the clean-up.

        :param url_rc: API URL to be written to configuration file
        :param key_rc: API key to be written to configuration file
        :param url_env: API URL to be written to environment variable
        :param key_env: API key to be written to environment variable
        :return: an instantiated CDS client object
        """
        rc_path = os.path.join(self.temp_dir.name, "cdsapi.rc")
        with open(rc_path, 'w') as fh:
            fh.write(f'url: {url_rc}\n')
            fh.write(f'key: {key_rc}\n')
        ClientUrlTest._erase_environment_variables()
        os.environ['CDSAPI_RC'] = rc_path
        if url_env is not None:
            os.environ['CDSAPI_URL'] = url_env
        if key_env is not None:
            os.environ['CDSAPI_KEY'] = key_env

    @staticmethod
    def _erase_environment_variables():
        for var in 'CDSAPI_URL', 'CDSAPI_KEY', 'CDSAPI_RC':
            if var in os.environ:
                del os.environ[var]


if __name__ == '__main__':
    unittest.main()
