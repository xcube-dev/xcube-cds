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
from xcube.util.jsonschema import JsonArraySchema, JsonBooleanSchema
from xcube.util.jsonschema import JsonIntegerSchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
from xcube_cds.constants import DEFAULT_NUM_RETRIES, ERA5_PARAMETERS
from xcube_cds.constants import CDS_DATA_OPENER_ID

import cdsapi


class CDSDataOpener(DataOpener):
    """A data opener for the Copernicus Climate Data Store"""

    def __init__(self):
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
        self._valid_data_ids = ('reanalysis-era5-single-levels-monthly-means',)

    ###########################################################################
    # DataOpener implementation

    def get_open_data_params_schema(self, data_id: Optional[str] = None) -> \
            JsonObjectSchema:

        self._validate_data_id(data_id, allow_none=True)

        era5_params = dict(
            dataset_name=JsonStringSchema(min_length=1,
                                          enum=list(self._valid_data_ids),
                                          default=self._valid_data_ids[0]),
            product_type=JsonArraySchema(
                items=(JsonStringSchema(
                    enum=['monthly_averaged_ensemble_members',
                          'monthly_averaged_ensemble_members_by_hour_of_day',
                          'monthly_averaged_reanalysis',
                          'monthly_averaged_reanalysis_by_hour_of_day', ])
                ),
                unique_items=True,
                default=['monthly_averaged_reanalysis'] # not supported?
            ),
            variable_names=JsonArraySchema(
                items=(JsonStringSchema(
                    min_length=1,
                    enum=[cds_api_name
                          for cds_api_name, _, _, _ in ERA5_PARAMETERS]
                )),
                unique_items=True
            ),
            crs=JsonStringSchema(nullable=True, default='WGS84',
                                 enum=[None, 'WGS84']),
            # N, W, S, E
            bbox=JsonArraySchema(items=(
                JsonNumberSchema(minimum=-90, maximum=90),
                JsonNumberSchema(minimum=-180, maximum=180),
                JsonNumberSchema(minimum=-90, maximum=90),
                JsonNumberSchema(minimum=-180, maximum=180))),
            spatial_res=JsonNumberSchema(minimum=0.25, maximum=10),
            time_range=JsonArraySchema(
                items=[JsonStringSchema(format='date-time'),
                       JsonStringSchema(format='date-time', nullable=True)]),
            time_period=JsonStringSchema(const='1M'),
            hours=JsonArraySchema(
                items=JsonIntegerSchema(minimum=0, maximum=23),
                unique_items=True,
                min_items=1
            ),
            months=JsonArraySchema(
                items=JsonIntegerSchema(minimum=1, maximum=12),
                unique_items=True,
                min_items=1
            ),
            years=JsonArraySchema(
                items=JsonIntegerSchema(minimum=1979, maximum=2020),
                unique_items=True,
                min_items=1
            ),
        )
        required = [
            'variable_names',
            'bbox',
            'spatial_res',
            'time_range',
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

        return dataset

    @staticmethod
    def _transform_params(plugin_params):
        """Transform supplied parameters to CDS API format.

        :param plugin_params: parameters in form expected by this plugin
        :return: parameters in form expected by the CDS API
        """

        # Translate our parameters to the CDS API scheme. Initially we use
        # default values for "month" and "time", and set "year" from the
        # compulsory time_range parameter.
        params_combined = {
            'product_type': plugin_params['product_type'] if 'product_type' in plugin_params else 'monthly_averaged_reanalysis',
            'variable': plugin_params['variable_names'],
            'year': CDSDataOpener._time_range_to_years(
                plugin_params['time_range']),
            'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09',
                      '10', '11', '12', ],
            'time': ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
                     '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
                     '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
                     '18:00', '19:00', '20:00', '21:00', '22:00', '23:00', ],
            'area': plugin_params['bbox'],
            # Note: the "grid" parameter is not via the web interface, but is
            # described at
            # https://confluence.ecmwf.int/display/CKB/ERA5%3A+Web+API+to+CDS+API .
            'grid': [plugin_params['spatial_res'],
                     plugin_params['spatial_res']],
            'format': 'netcdf'
        }

        # If any of the "years", "months", and "hours" parameters were passed,
        # they override the time specifications above.
        time_params = {
            k1: v1 for k1, v1 in [CDSDataOpener._transform_param(k0, v0)
                                  for k0, v0 in plugin_params.items()]
            if k1 is not None}
        params_combined.update(time_params)

        # Transform singleton list values into their single members, as
        # required by the CDS API.
        desingletonned = {
            k: (v[0] if isinstance(v, list) and len(v) == 1 else v)
            for k, v in params_combined.items()}

        return desingletonned

    @staticmethod
    def _transform_param(key, value):
        if key == 'hours':
            return 'time', list(map(lambda x: f'{x:02d}:00', value))
        elif key == 'months':
            return 'month', list(map(lambda x: f'{x:02d}', value))
        elif key == 'years':
            return 'year', list(map(lambda x: f'{x:04d}', value))
        else:
            return None, None

    @staticmethod
    def _time_range_to_years(range_array):
        year_start = int(range_array[0][:4])
        # Default end year value currently hard-coded for ERA5
        year_end = 2020 if range_array[1] is None else int(range_array[1][:4])
        return list(range(year_start, year_end + 1))

    def _validate_data_id(self, data_id, allow_none=False):
        if (data_id is None) and allow_none:
            return
        if data_id not in self._valid_data_ids:
            import traceback
            traceback.print_stack()
            raise ValueError(f'Unknown data id "{data_id}"')


class CDSDataStore(CDSDataOpener, DataStore):

    def __init__(self,
                 normalize_names: Optional[bool] = False,
                 num_retries: Optional[int] = DEFAULT_NUM_RETRIES,
                 **kwargs):
        super().__init__(**kwargs)
        self.normalize_names = normalize_names
        self.num_retries = num_retries

    ###########################################################################
    # DataStore implementation

    @classmethod
    def get_data_store_params_schema(cls) -> JsonObjectSchema:
        params = dict(
            normalize_names=JsonBooleanSchema(default=False)
        )

        # For now, let CDS API use defaults or environment variables for
        # most parameters.
        cds_params = dict(
            num_retries=JsonIntegerSchema(default=DEFAULT_NUM_RETRIES,
                                          minimum=0),
        )

        params.update(cds_params)
        return JsonObjectSchema(
            properties=params,
            required=None,
            additional_properties=False
        )

    @classmethod
    def get_type_ids(cls) -> Tuple[str, ...]:
        return TYPE_ID_DATASET,

    def get_data_ids(self, type_id: Optional[str] = None) -> Iterator[str]:
        self._assert_valid_type_id(type_id)
        return iter(self._valid_data_ids)

    def has_data(self, data_id: str) -> bool:
        return data_id in self._valid_data_ids

    def describe_data(self, data_id: str) -> DataDescriptor:
        self._validate_data_id(data_id)
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
        self._assert_valid_opener_id(data_id)
        return CDS_DATA_OPENER_ID,

    def get_open_data_params_schema(self, data_id: Optional[str] = None,
                                    opener_id: Optional[str] = None) -> \
            JsonObjectSchema:
        self._assert_valid_opener_id(opener_id)
        self._validate_data_id(data_id)
        return super().get_open_data_params_schema(data_id)

    def open_data(self, data_id: str, opener_id: Optional[str] = None,
                  **open_params) -> xr.Dataset:
        self._assert_valid_opener_id(opener_id)
        self._validate_data_id(data_id)
        dataset = super().open_data(data_id, **open_params)
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
