# MIT License
#
# Copyright (c) 2022 Brockmann Consult GmbH
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

""" Unit tests for the seaice dataset in the CDS Store

See test_store.py for further documentation.
"""

from copy import deepcopy
from typing import Optional
import unittest

from test.mocks import CDSClientMock
from xcube_cds.store import CDSDataStore
from xcube_cds.datasets.satellite_sea_ice_thickness import SeaIceThicknessHandler

_CDS_API_URL = 'dummy'
_CDS_API_KEY = 'dummy'

_ENVISAT_DATA_ID = 'satellite-sea-ice-thickness:envisat'
_CRYOSAT_2_DATA_ID = 'satellite-sea-ice-thickness:cryosat-2'

_OPEN_PARAMS_SCHEMA_TEMPLATE = {
    'type': 'object',
    'properties':
        {
            'time_range':
                {
                    'type': 'array',
                    'items':
                        [
                            {
                                'type': 'string',
                                'format': 'date',
                            },
                            {
                                'type': 'string',
                                'format': 'date',
                            }
                        ]
                },
            'variable_names':
                {
                    'type': 'array',
                    'default': ['all'],
                    'items':
                        {
                            'type': 'string',
                            'default': 'all',
                            'enum': ['all'],
                            'minLength': 0
                        },
                    'uniqueItems': True
                },
            'type_of_record':
                {
                    'type': 'string',
                    'default': 'cdr',
                    'title': 'Type of record',
                    'description': 'This dataset combines a Climate Data Record (CDR), '
                                   'which has sufficient length, consistency, and '
                                   'continuity to be used to assess climate '
                                   'variability and change, and an Interim Climate '
                                   'Data Record (ICDR), which provides regular '
                                   'temporal extensions to the CDR and where '
                                   'consistency with the CDR is expected but not '
                                   'extensively checked. The ICDR is based on '
                                   'observations from CryoSat-2 only (from April 2015 '
                                   'onward).',
                },
            'version':
                {
                    'type': 'string',
                    'default': '2.0',
                    'enum': ['2.0', '1.0'],
                    'title': 'Data version'
                }
        },
    'additionalProperties': False,
    'required': ['time_range']
}
_ENVISAT_PARAMS_SCHEMA = deepcopy(_OPEN_PARAMS_SCHEMA_TEMPLATE)
_ENVISAT_PARAMS_SCHEMA['properties']['time_range']['items'][0]['minDate'] = '2002-10-01'
_ENVISAT_PARAMS_SCHEMA['properties']['time_range']['items'][0]['maxDate'] = '2010-10-31'
_ENVISAT_PARAMS_SCHEMA['properties']['time_range']['items'][1]['minDate'] = '2002-10-01'
_ENVISAT_PARAMS_SCHEMA['properties']['time_range']['items'][1]['maxDate'] = '2010-10-31'
_ENVISAT_PARAMS_SCHEMA['properties']['type_of_record']['enum'] = ['cdr']

_CRYOSAT_PARAMS_SCHEMA = deepcopy(_OPEN_PARAMS_SCHEMA_TEMPLATE)
_CRYOSAT_PARAMS_SCHEMA['properties']['time_range']['items'][0]['minDate'] = '2010-11-01'
_CRYOSAT_PARAMS_SCHEMA['properties']['time_range']['items'][1]['minDate'] = '2010-11-01'
_CRYOSAT_PARAMS_SCHEMA['properties']['type_of_record']['enum'] = ['cdr', 'icdr']


class CDSSeaIceThicknessHandlerTest(unittest.TestCase):

    def setUp(self) -> None:
        self.sea_ice_handler = SeaIceThicknessHandler()

    def testGetSupportedDataIds(self):
        ids = self.sea_ice_handler.get_supported_data_ids()
        self.assertEqual({_ENVISAT_DATA_ID, _CRYOSAT_2_DATA_ID}, set(ids))

    def testGetHumanReadableDataId(self):
        self.assertEqual('Sea ice thickness (Envisat)',
                         self.sea_ice_handler.get_human_readable_data_id(
                             _ENVISAT_DATA_ID
                         ))
        self.assertEqual('Sea ice thickness (CryoSat-2)',
                         self.sea_ice_handler.get_human_readable_data_id(
                             _CRYOSAT_2_DATA_ID
                         ))

    def test_describe_envisat_data(self):
        self.assertDescriptor(_ENVISAT_DATA_ID, '2002-10-01', '2010-10-31')

    def test_describe_cryosat_data(self):
        self.assertDescriptor(_CRYOSAT_2_DATA_ID, '2010-11-01', None)

    def assertDescriptor(self,
                         data_id: str,
                         start_date: str,
                         end_date: Optional[str]):
        descriptor = self.sea_ice_handler.describe_data(data_id)
        self.assertEqual(data_id, descriptor.data_id)
        self.assertEqual('EPSG:6931', descriptor.crs)
        self.assertEqual((-180, 16.6239, 180, 90), descriptor.bbox)
        self.assertEqual(25.0, descriptor.spatial_res)
        self.assertEqual(start_date, descriptor.time_range[0])
        if end_date is None:
            self.assertIsNone(descriptor.time_range[1])
        else:
            self.assertEqual(end_date, descriptor.time_range[1])
        self.assertEqual({'sea_ice_thickness', 'quality_flag', 'status_flag',
                           'uncertainty'},
                          set(descriptor.data_vars.keys()))
        self.assertEqual({'time', 'time_bnds', 'xc', 'yc',
                           'Lambert_Azimuthal_Grid'},
                          set(descriptor.coords.keys()))

    def test_get_open_data_params_schema_envisat(self):
        envisat_schema = self.sea_ice_handler.get_open_data_params_schema(
            _ENVISAT_DATA_ID
        )
        self.assertEqual(_ENVISAT_PARAMS_SCHEMA, envisat_schema.to_dict())

    def test_get_open_data_params_schema_cryosat(self):
        cryosat_schema = self.sea_ice_handler.get_open_data_params_schema(
            _CRYOSAT_2_DATA_ID
        )
        self.assertEqual(_CRYOSAT_PARAMS_SCHEMA, cryosat_schema.to_dict())


class CdsSeaIceThicknessStoreTest(unittest.TestCase):

    def setUp(self) -> None:
        self.store = CDSDataStore(
            client_class=CDSClientMock,
            endpoint_url=_CDS_API_URL,
            cds_api_key=_CDS_API_KEY
        )

    def test_get_data_ids(self):
        ids = list(self.store.get_data_ids())
        self.assertIn(_ENVISAT_DATA_ID, ids)
        self.assertIn(_CRYOSAT_2_DATA_ID, ids)

    def test_get_open_params_schema(self):
        envisat_open_params = self.store.get_open_data_params_schema(_ENVISAT_DATA_ID)
        self.assertEqual(_ENVISAT_PARAMS_SCHEMA, envisat_open_params.to_dict())
        cryosat_open_params = self.store.get_open_data_params_schema(_CRYOSAT_2_DATA_ID)
        self.assertEqual(_CRYOSAT_PARAMS_SCHEMA, cryosat_open_params.to_dict())

    def test_describe_envisat_data(self):
        self.assertDescriptor(_ENVISAT_DATA_ID, '2002-10-01', '2010-10-31')

    def test_describe_cryosat_data(self):
        self.assertDescriptor(_CRYOSAT_2_DATA_ID, '2010-11-01', None)

    def assertDescriptor(self,
                         data_id: str,
                         start_date: str,
                         end_date: Optional[str]):
        descriptor = self.store.describe_data(data_id)
        self.assertEqual(data_id, descriptor.data_id)
        self.assertEqual('EPSG:6931', descriptor.crs)
        self.assertEqual((-180, 16.6239, 180, 90), descriptor.bbox)
        self.assertEqual(25.0, descriptor.spatial_res)
        self.assertEqual(start_date, descriptor.time_range[0])
        if end_date is None:
            self.assertIsNone(descriptor.time_range[1])
        else:
            self.assertEqual(end_date, descriptor.time_range[1])
        self.assertEqual({'sea_ice_thickness', 'quality_flag', 'status_flag',
                           'uncertainty'},
                          set(descriptor.data_vars.keys()))
        self.assertEqual({'time', 'time_bnds', 'xc', 'yc',
                           'Lambert_Azimuthal_Grid'},
                          set(descriptor.coords.keys()))

    def test_open_envisat(self):
        dataset = self.store.open_data(
            _ENVISAT_DATA_ID,
            time_range=['2005-03-01', '2005-04-30']
        )
        self.assertTrue('sea_ice_thickness' in dataset.variables)
        self.assertEqual(2, len(dataset.variables['time']))
        self.assertEqual('2005-03-01T00:00:00',
                         dataset.attrs['time_coverage_start'])
        self.assertEqual('2005-04-30T23:59:59.999999',
                         dataset.attrs['time_coverage_end'])
        description = self.store.describe_data(_ENVISAT_DATA_ID)
        self.assertCountEqual(description.coords.keys(),
                              map(str, dataset.coords))
        self.assertCountEqual(description.data_vars.keys(),
                              map(str, dataset.data_vars))

    def test_open_cryosat_2(self):
        dataset = self.store.open_data(
            _CRYOSAT_2_DATA_ID,
            time_range=['2016-03-01', '2016-04-30']
        )
        self.assertTrue('sea_ice_thickness' in dataset.variables)
        self.assertEqual(2, len(dataset.variables['time']))
        self.assertEqual('2016-03-01T00:00:00',
                         dataset.attrs['time_coverage_start'])
        self.assertEqual('2016-04-30T23:59:59.999999',
                         dataset.attrs['time_coverage_end'])
        description = self.store.describe_data(_CRYOSAT_2_DATA_ID)
        self.assertCountEqual(description.coords.keys(),
                              map(str, dataset.coords))
        self.assertCountEqual(description.data_vars.keys(),
                              map(str, dataset.data_vars))
