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

import atexit
import os.path
import re
import shutil
import tempfile
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
from xcube_cds.constants import DEFAULT_NUM_RETRIES, ERA5_PARAMETERS
from xcube_cds.constants import CDS_DATA_OPENER_ID

import cdsapi


class CDSDataOpener(DataOpener):

    def __init__(self, normalize_names: Optional[bool] = False):
        self.normalize_names = normalize_names

        # Create a temporary directory to hold downloaded NetCDF files and
        # a hook to delete it when the interpreter exits. xarray.open reads
        # data lazily so we can't just delete the file after returning the
        # Dataset. We could also use weakref hooks to delete individual files
        # when the corresponding object is garbage collected, but even then
        # the directory is useful to group the files and offer an extra
        # assurance that they will be deleted.
        tempdir = tempfile.mkdtemp()

        def delete_tempdir():
            shutil.rmtree(tempdir, ignore_errors=True)
        atexit.register(delete_tempdir)
        self._tempdir = tempdir

    ###########################################################################
    # DataOpener implementation

    def get_open_data_params_schema(self, data_id: Optional[str] = None) ->\
            JsonObjectSchema:

        valid_data_ids = ['reanalysis-era5-single-levels-monthly-means']
        if data_id not in valid_data_ids:
            raise ValueError(f'Unknown data id "{data_id}"')

        era5_params = dict(
            dataset_name=JsonStringSchema(min_length=1),
            product_type=JsonArraySchema(
                items=(JsonStringSchema(
                    enum=['monthly_averaged_ensemble_members',
                          'monthly_averaged_ensemble_members_by_hour_of_day',
                          'monthly_averaged_reanalysis',
                          'monthly_averaged_reanalysis_by_hour_of_day', ])
                )
            ),
            variable_names=JsonArraySchema(
                items=(JsonStringSchema(
                    enum=[cds_api_name
                          for cds_api_name, _, _, _ in ERA5_PARAMETERS]
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
            'hours',
            'months',
            'years',
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

        # We can't generate a safe unique filename (since the file is created
        # by client.retrieve, so name generation and file creation won't be
        # atomic). Instead we atomically create a subdirectory of the temporary
        # directory for the single file.
        subdir = tempfile.mkdtemp(dir=self._tempdir)
        file_path = os.path.join(subdir, 'data.nc')
        cds_api_params = CDSDataOpener._transform_params(open_params)

        # This call returns a Result object, which at present we make
        # no use of.
        client.retrieve(data_id, cds_api_params, file_path)

        # decode_cf is the default, but it's clearer to make it explicit.
        dataset = xr.open_dataset(file_path, decode_cf=True)

        # The API doesn't close the session automatically, so we need to
        # do it explicitly here to avoid leaving an open socket.
        client.session.close()

        if self.normalize_names:
            rename_dict = {}
            for name in dataset.data_vars.keys():
                normalized_name = re.sub('\W|^(?=\d)', '_', name)
                if name != normalized_name:
                    rename_dict[name] = normalized_name
            dataset_renamed = dataset.rename_vars(rename_dict)
            return dataset_renamed
        else:
            return dataset

    @staticmethod
    def _transform_params(plugin_params):
        """
        Transform supplied parameters to CDS API format.

        :param plugin_params: parameters in form expected by this plugin
        :return: parameters in form expected by the CDS API
        """

        # Transform our key names and value formats to those expected by
        # the CDS API.
        transformed = {kv[0]: kv[1]
                       for kv in [CDSDataOpener._transform_param(k, v)
                                  for k, v in plugin_params.items()]}

        # Transform singleton list values into their single members, as
        # required by the CDS API.
        desingletonned = \
            {k: (v[0] if isinstance(v, list) and len(v) == 1 else v)
             for k, v in transformed.items()}

        # Add output format parameter.
        desingletonned['format'] = 'netcdf'
        return desingletonned

    @staticmethod
    def _transform_param(key, value):
        if key == 'product_type':
            return key, value
        if key == 'variable_names':
            return 'variable', value
        elif key == 'hours':
            return 'time', list(map(lambda x: f'{x:02d}:00', value))
        elif key == 'months':
            return 'month', list(map(lambda x: f'{x:02d}', value))
        elif key == 'years':
            return 'year', list(map(lambda x: f'{x:04d}', value))
        elif key == 'bbox':
            return 'area', value
        else:
            raise ValueError(f'Unhandled key "{key}"')


class CDSDataStore(CDSDataOpener, DataStore):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
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
        if data_id not in self._dataset_ids:
            raise ValueError(f'data_id "{data_id}" not supported.')
        return DatasetDescriptor(
            data_id=data_id,
            data_vars=self._create_era5_variable_descriptors())

    @staticmethod
    def _create_era5_variable_descriptors():
        return [
            VariableDescriptor(
                name=netcdf_name,
                # dtype string format not formally defined as of 2020-06-18.
                # t2m is actually stored as a short with scale and offset in
                # the NetCDF file, but converted to float by xarray on opening:
                # see http://xarray.pydata.org/en/stable/io.html .
                dtype='float32',
                dims=('time', 'latitude', 'longitude'),
                description=long_name,
                attrs=dict(units=units, long_name=long_name))
            for (api_name, netcdf_name, units, long_name) in ERA5_PARAMETERS
        ]

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
