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

from typing import Iterator, Tuple, Optional

import xarray as xr

from xcube.core.store.accessor import DataOpener
from xcube.core.store.descriptor import DataDescriptor
from xcube.core.store.descriptor import DatasetDescriptor
from xcube.core.store.descriptor import TYPE_ID_DATASET
from xcube.core.store.descriptor import VariableDescriptor
from xcube.core.store.store import DataStore
from xcube.core.store.store import DataStoreError
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonIntegerSchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
from xcube_cds.constants import DEFAULT_NUM_RETRIES
from xcube_cds.constants import CDS_DATA_OPENER_ID

import cdsapi

class CDSDataOpener(DataOpener):

    ###########################################################################
    # DataOpener implementation

    def get_open_data_params_schema(self, data_id: Optional[str] = None) ->\
            JsonObjectSchema:

        valid_data_ids = ['reanalysis-era5-single-levels-monthly-means']
        if data_id not in valid_data_ids:
            raise ValueError(f'Unknown data id "{data_id}"')

        era5_params = dict(
            dataset_name=JsonStringSchema(min_length=1),
            product_type=JsonStringSchema(
                enum=['monthly_averaged_reanalysis_by_hour_of_day']),
            variable_names=JsonArraySchema(
                items=(JsonStringSchema(
                    enum=['2m_temperature']
                )),
                unique_items=True
            ),
            hours=JsonArraySchema(
                items=(JsonIntegerSchema(minimum=0, maximum=23),),
                unique_items=True,
                min_items=1
            ),
            months=JsonArraySchema(
                items=(JsonIntegerSchema(minimum=1, maximum=12),),
                unique_items=True,
                min_items=1
            ),
            years=JsonArraySchema(
                items=(JsonIntegerSchema(minimum=1979, maximum=2020),),
                unique_items=True,
                min_items=1
            ),
            # N, W, S, E
            bbox=JsonArraySchema(items=(
                JsonNumberSchema(minimum=-90, maximum=90),
                JsonNumberSchema(minimum=-180, maximum=180),
                JsonNumberSchema(minimum=-90, maximum=90),
                JsonNumberSchema(minimum=-180, maximum=180)))
        )
        required = [
            'product_type',
            'variable_names',
            'years',
            'months',
            'hours'
        ]
        return JsonObjectSchema(
            properties=dict(
                **era5_params,
            ),
            required=required
        )

    def open_data(self, data_id: str, **open_params) -> xr.Dataset:
        schema = self.get_open_data_params_schema(data_id)
        schema.validate_instance(open_params)

        client = cdsapi.Client()

        file_path = 'fixme.nc'
        client.retrieve(data_id, CDSDataOpener._transform_params(open_params),
                        file_path)
        return xr.open_dataset(file_path, decode_cf=True)

    @staticmethod
    def _transform_params(plugin_params):
        """
        Transform supplied parameters to CDS API format.

        :param plugin_params: parameters in form expected by this plugin
        :return: parameters in form expected by the CDS API
        """

        transformed = {kv[0]: kv[1]
                       for kv in [CDSDataOpener._transform_param(k, v)
                                  for k, v in plugin_params.items()]}
        desingletonned = {k: CDSDataOpener._strip_singleton(v)
                          for k, v in transformed.items()}
        desingletonned['format'] = 'netcdf'
        return desingletonned

    @staticmethod
    def _transform_param(key_in, value_in):
        # TODO: implement the transformations
        # convert hour integers to hh:mm format
        # convert month integers to two-digit strings
        # add 'format' : 'netcdf'
        # rename keys as required
        if key_in == 'variable_names':
            pass
        elif key_in == 'hours':
            pass
        elif key_in == 'months':
            pass
        elif key_in == 'years':
            pass
        elif key_in == 'bbox':
            pass

        return key_in, value_in

    @staticmethod
    def _strip_singleton(sequence):
        return sequence[0] if len(sequence) == 0 else sequence


class CDSDataStore(CDSDataOpener, DataStore):

    def __init__(self):
        self._dataset_ids = 'reanalysis-era5-single-levels-monthly-means',

    ###########################################################################
    # DataStore implementation

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        # For now, let CDS API use defaults or environment variables for
        # most parameters.
        cds_params = dict(
            num_retries=JsonIntegerSchema(default=DEFAULT_NUM_RETRIES,
                                          minimum=0),
        )
        required = None
        return JsonObjectSchema(
            properties=cds_params,
            required=required,
            additional_properties=False
        )

    @classmethod
    def get_type_ids(cls) -> Tuple[str, ...]:
        return TYPE_ID_DATASET,

    def get_data_ids(self, type_id: Optional[str] = None) -> Iterator[str]:
        self._assert_valid_type_id(type_id)
        return iter(self._dataset_ids)

    def has_data(self, data_id: str) -> bool:
        return data_id in self._dataset_ids

    def describe_data(self, data_id: str) -> DataDescriptor:
        return DatasetDescriptor(
            data_id=data_id,
            data_vars=[VariableDescriptor(
                name="t2m",
                # dtype string format not formally defined as of 2020-06-18.
                # t2m is actually stored as a short with scale and offset in
                # the NetCDF file, but converted to float by xarray on opening:
                # see http://xarray.pydata.org/en/stable/io.html .
                dtype='float32',
                dims=('time', 'latitude', 'longitude'),
                attrs=dict(units='K', long_name='2 metre temperature'))])

    # noinspection PyTypeChecker
    def search_data(self, type_id: Optional[str] = None, **search_params) -> \
            Iterator[DataDescriptor]:
        self._assert_valid_type_id(type_id)
        raise NotImplementedError()

    def get_data_opener_ids(self, data_id: Optional[str] = None,
                            type_id: Optional[str] = None) -> \
            Tuple[str, ...]:
        self._assert_valid_type_id(type_id)
        return CDS_DATA_OPENER_ID,

    def get_open_data_params_schema(self, data_id: Optional[str] = None,
                                    opener_id: Optional[str] = None) -> \
            JsonObjectSchema:
        self._assert_valid_opener_id(opener_id)

        return super().get_open_data_params_schema(data_id)

    def open_data(self, data_id: str, opener_id: Optional[str] = None,
                  **open_params) -> xr.Dataset:
        self._assert_valid_opener_id(opener_id)
        return super().open_data(data_id, **open_params)

    ###########################################################################
    # Implementation helpers

    @staticmethod
    def _assert_valid_type_id(type_id):
        if type_id is not None and type_id != TYPE_ID_DATASET:
            raise DataStoreError(
                f'Data type identifier must be "{TYPE_ID_DATASET}", '
                f'but got "{type_id}"')

    @staticmethod
    def _assert_valid_opener_id(opener_id):
        if opener_id is not None and opener_id != CDS_DATA_OPENER_ID:
            raise DataStoreError(
                f'Data opener identifier must be "{CDS_DATA_OPENER_ID}"'
                f'but got "{opener_id}"')
