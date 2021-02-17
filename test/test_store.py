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
from xcube.core.store import DataDescriptor
from xcube.core.store import DataStoreError
from xcube.core.store import TYPE_SPECIFIER_CUBE
from xcube.core.store import TYPE_SPECIFIER_DATASET

from test.mocks import CDSClientMock
from xcube_cds.constants import CDS_DATA_OPENER_ID
from xcube_cds.datasets.reanalysis_era5 import ERA5DatasetHandler
from xcube_cds.store import CDSDataOpener
from xcube_cds.store import CDSDataStore
from xcube_cds.store import CDSDatasetHandler

_CDS_API_URL = 'dummy'
_CDS_API_KEY = 'dummy'


class CDSStoreTest(unittest.TestCase):

    def test_invalid_data_id(self):
        store = CDSDataStore(cds_api_url=_CDS_API_URL,
                             cds_api_key=_CDS_API_KEY)
        with self.assertRaises(ValueError):
            store.open_data(
                'this-data-id-does-not-exist',
                variable_names=['2m_temperature'],
                hours=[0], months=[1], years=[2019]
            )

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
        result = list(store.search_data('dataset'))
        self.assertTrue(len(result) > 0)

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

    def test_has_data_false(self):
        self.assertFalse(CDSDataStore().has_data('nonexistent data ID'))

    def test_get_data_opener_ids_invalid_type_id(self):
        with self.assertRaises(DataStoreError):
            CDSDataStore().get_data_opener_ids(CDS_DATA_OPENER_ID,
                                               'this is an invalid ID')

    def test_get_data_opener_ids_invalid_opener_id(self):
        with self.assertRaises(ValueError):
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

    def test_era5_transform_params_empty_variable_list(self):
        handler = ERA5DatasetHandler()
        with self.assertRaises(ValueError):
            handler.transform_params(
                dict(bbox=[0, 0, 10, 10], spatial_res=0.5, variable_names=[]),
                'reanalysis-era5-land'
            )


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
