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
import datetime
import os
import re
import shutil
import tempfile
from abc import abstractmethod, ABC
from typing import Iterator, Tuple, List, Optional, Dict, Any, Union

import cdsapi
import dateutil.parser
import dateutil.relativedelta
import dateutil.rrule
import numpy as np
import xarray as xr

import xcube.core.normalize
from xcube.core.store import DataDescriptor
from xcube.core.store import DataOpener
from xcube.core.store import DataStore
from xcube.core.store import DataStoreError
from xcube.core.store import TYPE_ID_DATASET
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonBooleanSchema
from xcube.util.jsonschema import JsonIntegerSchema
from xcube.util.jsonschema import JsonNumberSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema
from xcube.util.undefined import UNDEFINED
from xcube_cds.constants import CDS_DATA_OPENER_ID
from xcube_cds.constants import DEFAULT_NUM_RETRIES


class CDSDatasetHandler(ABC):
    """A handler for one or more CDS datasets

    This class defines abstract methods which must be implemented by
    dataset handler subclasses, and concrete utility methods for use by
    those subclasses."""

    @abstractmethod
    def get_supported_data_ids(self) -> List[str]:
        """Return the data IDs supported by a handler

        There need not be a one-to-one mapping between data IDs and CDS
        datasets. In particular, request parameters can be encoded within the
        data ID itself so that a single CDS dataset is represented by
        multiple data IDs.

        :return: the data IDs supported by this handler
        """
        pass

    @abstractmethod
    def get_open_data_params_schema(self, data_id: str) -> \
            JsonObjectSchema:
        """Return the open parameters schema for a specified dataset.

        Note that the data_id is not optional here: CDSDataOpener handles
        the data_id == None case rather than passing it on to a handler.

        :param data_id: a dataset identifier
        :return: schema for open parameters for the dataset identified by
            data_id
        """
        pass

    @abstractmethod
    def get_human_readable_data_id(self, data_id: str) -> str:
        """Return a human-readable identifier corresponding to a data ID

        :param data_id: a data ID
        :return: a corresponding human-readable representation, suitable for
            display in a GUI
        """
        pass

    @abstractmethod
    def describe_data(self, data_id: str) -> DataDescriptor:
        """Return a data descriptor for a given data ID.

        :param data_id: a data ID
        :return: a corresponding descriptor
        """
        pass

    @abstractmethod
    def transform_params(self, opener_params: Dict, data_id: str) -> \
            Tuple[str, Dict[str, Any]]:
        """Transform opener parameters into CDS API parameters

        The caller is responsible for ensuring that all required parameters
        are present in opener_params; default values may be filled in from
        those specified in the opener parameters schema.

        :param opener_params: opener parameters conforming to the opener
            parameter schema for the given data_id
        :param data_id: a valid data identifier
        :return: CDS API request parameters corresponding to the specified
            opener parameters
        """
        pass

    @abstractmethod
    def read_file(self, dataset_name: str,
                  cds_api_params: Dict[str, Union[str, List[str]]],
                  file_path: str, temp_dir: str) -> xr.Dataset:
        """Read a file downloaded via the CDS API as into an xarray Dataset

        :param dataset_name: the CDS name of the dataset (note that this
            may not be the same as the data ID used by the xcube data store)
        :param cds_api_params: the CDS API parameters which produced the
            downloaded file
        :param file_path: the path to the downloaded file
        :param temp_dir: a temporary directory which the handler can use
            as working space. The handler has exclusive use of the directory.
            The directory is not deleted until the interpreter exits, so
            the returned dataset can read from it lazily if required.
        :return: a dataset corresponding to the specified file
        """
        pass

    def transform_time_params(self, params: Dict[str, List[int]]) -> Dict:
        """Convert a dictionary of time specifiers to CDS form.

        This method renames the pluralized keys to singular (hours -> hour,
        etc.) and converts the integer values to the string format expected
        by CDS.

        :param params: a dictionary containing time specifier keys
            ('hours', 'days', 'months', or 'years') and list-of-integer values
        :return: a dictionary with all time-specifier key-value pairs
            converted to CDS API form (keys in singular, values as lists of
            strings) and all other key-value pairs omitted
        """
        return {
            k1: v1 for k1, v1 in [self.transform_time_param(k0, v0)
                                  for k0, v0 in params.items()]
            if k1 is not None}

    @staticmethod
    def transform_time_param(key: str, value: List[int]) -> \
            Tuple[Optional[str], Optional[List[str]]]:
        """Convert an hours/days/months/years time specifier to CDS form

        This method renames the pluralized keys to singular (hours -> hour,
        etc.) and converts the integer values to the string format expected
        by CDS.

        :param key: a time specifier key
            ('hours', 'days', 'months', or 'years')
        :param value: a list of integers corresponding to the time specifier
        :return: a tuple of modified time specifier key and list of strings, or
            (None, None) if the key was not recognized
        """

        if key == 'hours':
            return 'time', list(map(lambda x: f'{x:02d}:00', value))
        if key == 'days':
            return 'day', list(map(lambda x: f'{x:02d}', value))
        elif key == 'months':
            return 'month', list(map(lambda x: f'{x:02d}', value))
        elif key == 'years':
            return 'year', list(map(lambda x: f'{x:04d}', value))
        else:
            return None, None

    @staticmethod
    def convert_time_range(time_range: List[str]) -> \
            Dict[str, List[int]]:
        """Convert a time range to a CDS-style time specification.

        This method converts a time range specification (i.e. a straightforward
        pair of "start time" and "end time") into the closest corresponding
        specification for CDS datasets such as ERA5 (which allow orthogonal
        selection of subsets of years, months, days, and hours). "Closest"
        here means the narrowest selection which will cover the entire
        requested time range, although it will often cover significantly more.
        For example, the range 2000-12-31 to 2002-01-02 would be translated
        to a request for all of 2000-2002, since every hour, day, and month
        must be selected.

        Note that the output still requires further transformation before
        being passed to the CDS API: the key names must be replaced, the
        integers formatted into strings, and any singleton lists replaced
        by their content.

        :param time_range: a length-2 list of ISO-8601 date/time strings
        :return: a dictionary with keys 'hours', 'days', 'months', and
            'years', and values which are lists of ints
        """

        # Python type hints don't support list lengths, so we check this
        # manually.
        if len(time_range) != 2:
            raise ValueError(f'time_range must have a length of 2, '
                             'not {len(time_range)}.')

        time0 = dateutil.parser.isoparse(time_range[0])
        time1 = datetime.datetime.now() if time_range[1] is None \
            else dateutil.parser.isoparse(time_range[1])

        # We use datetime's recurrence rule features to enumerate the
        # hour / day / month numbers which intersect with the selected time
        # range.

        hour0 = datetime.datetime(time0.year, time0.month, time0.day,
                                  time0.hour, 0)
        hour1 = datetime.datetime(time1.year, time1.month, time1.day,
                                  time1.hour, 59)
        hours = [dt.hour for dt in dateutil.rrule.rrule(
            freq=dateutil.rrule.HOURLY, count=24,
            dtstart=hour0, until=hour1)]
        hours.sort()

        day0 = datetime.datetime(time0.year, time0.month, time0.day, 0, 1)
        day1 = datetime.datetime(time1.year, time1.month, time1.day, 23, 59)
        days = [dt.day for dt in dateutil.rrule.rrule(
            freq=dateutil.rrule.DAILY, count=31, dtstart=day0, until=day1)]
        days = sorted(set(days))

        month0 = datetime.datetime(time0.year, time0.month, 1)
        month1 = datetime.datetime(time1.year, time1.month, 28)
        months = [dt.month for dt in dateutil.rrule.rrule(
            freq=dateutil.rrule.MONTHLY, count=12,
            dtstart=month0, until=month1)]
        months.sort()

        years = list(range(time0.year, time1.year + 1))

        return dict(hours=hours,
                    days=days,
                    months=months, years=years)

    @staticmethod
    def unwrap_singleton_values(dictionary: dict) -> dict:
        """Replace singleton values in a dictionary with their contents

        This method is useful when preparing parameters for the CDS API,
        which expects a bare value rather than a singleton list whenever
        a single value is to be passed for a usually list-valued parameter.

        :param dictionary: any dictionary
        :return: the input dictionary with any singleton list values unwrapped
        """
        return {
            k: (v[0] if isinstance(v, list) and len(v) == 1 else v)
            for k, v in dictionary.items()}

    @staticmethod
    def combine_netcdf_time_limits(paths: List[str]) -> Dict[str, str]:
        """Return the overall time limit attributes for a list of NetCDF files.

        Take a list of paths to NetCDF files. Return a dictionary in which
        the key 'time_coverage_start' has the value of the earliest
        'time_coverage_start' global attribute value in the specified files,
        and 'time_coverage_end' has the value of the latest
        'time_coverage_end' global attribute value in the specified files.

        Intended use: set correct values for the aforementioned attributes
        when combining multiple NetCDF files into a single dataset.

        :param paths: paths to NetCDF files
        :return: dictionary with keys 'time_coverage_start' and
                 'time_coverage_end'
        """

        start_key = 'time_coverage_start'
        end_key = 'time_coverage_end'
        starts, ends = [], []
        for path in paths:
            with xr.open_dataset(path) as ds:
                starts.append(ds.attrs[start_key])
                ends.append(ds.attrs[end_key])

        # Since the time specifiers are in ISO-8601, we can find the minimum
        # and maximum using the natural string ordering.
        return {start_key: min(starts), end_key: max(ends)}


class CDSDataOpener(DataOpener):
    """A data opener for the Copernicus Climate Data Store"""

    def __init__(self, normalize_names: Optional[bool] = False):
        self._normalize_names = normalize_names
        self._create_temporary_directory()
        self._handler_registry: Dict[str, CDSDatasetHandler] = {}
        from xcube_cds.datasets.reanalysis_era5 import ERA5DatasetHandler
        self._register_dataset_handler(ERA5DatasetHandler())
        from xcube_cds.datasets.satellite_soil_moisture \
            import SoilMoistureHandler
        self._register_dataset_handler(SoilMoistureHandler())

    def _register_dataset_handler(self, handler: CDSDatasetHandler):
        for data_id in handler.get_supported_data_ids():
            self._handler_registry[data_id] = handler

    def _create_temporary_directory(self):
        # Create a temporary directory to hold downloaded files and a hook to
        # delete it when the interpreter exits. xarray.open reads data lazily
        # so we can't just delete the file after returning the Dataset. We
        # could also use weakref hooks to delete individual files when the
        # corresponding object is garbage collected, but even then the
        # directory is useful to group the files and offer an extra assurance
        # that they will be deleted.
        tempdir = tempfile.mkdtemp()

        def delete_tempdir():
            shutil.rmtree(tempdir, ignore_errors=True)

        atexit.register(delete_tempdir)
        self._tempdir = tempdir

    ###########################################################################
    # DataOpener implementation

    def get_open_data_params_schema(self, data_id: Optional[str] = None) -> \
            JsonObjectSchema:
        self._validate_data_id(data_id, allow_none=True)
        return self._get_default_open_params_schema() \
            if data_id is None \
            else (self._handler_registry[data_id].
                  get_open_data_params_schema(data_id))

    def _get_default_open_params_schema(self) -> JsonObjectSchema:
        params = dict(
            dataset_name=JsonStringSchema(
                min_length=1,
                enum=list(self._handler_registry.keys())),
            variable_names=JsonArraySchema(
                items=(JsonStringSchema(min_length=0)),
                unique_items=True
            ),
            crs=JsonStringSchema(),
            # W, S, E, N
            bbox=JsonArraySchema(items=(
                JsonNumberSchema(minimum=-180, maximum=180),
                JsonNumberSchema(minimum=-90, maximum=90),
                JsonNumberSchema(minimum=-180, maximum=180),
                JsonNumberSchema(minimum=-90, maximum=90))),
            spatial_res=JsonNumberSchema(),
            time_range=JsonArraySchema(
                items=[JsonStringSchema(format='date-time'),
                       JsonStringSchema(format='date-time', nullable=True)]),
            time_period=JsonStringSchema(),
        )
        required = [
            'variable_names',
            'bbox',
            'spatial_res',
            'time_range',
        ]
        return JsonObjectSchema(
            properties=params,
            required=required
        )

    def open_data(self, data_id: str, **open_params) -> xr.Dataset:
        schema = self.get_open_data_params_schema(data_id)
        schema.validate_instance(open_params)
        handler = self._handler_registry[data_id]

        # Fill in defaults from the schema
        props = self.get_open_data_params_schema(data_id).properties
        all_open_params = {k: props[k].default for k in props
                           if props[k].default is not UNDEFINED}
        all_open_params.update(open_params)

        # Disable PyCharm's inspection which thinks False and [] are equivalent
        # noinspection PySimplifyBooleanCheck
        if open_params['variable_names'] == []:
            # The CDS API requires at least one variable to be selected,
            # so in order to return an empty dataset we have to construct
            # it ourselves.
            return self._create_empty_dataset(all_open_params)
        else:
            dataset_name, cds_api_params = \
                handler.transform_params(all_open_params, data_id)
            return self._open_data_with_handler(handler, dataset_name,
                                                cds_api_params)

    def _create_empty_dataset(self, open_params: dict) -> xr.Dataset:
        """Make a dataset with space and time dimensions but no data variables

        :param open_params: opener parameters
        :return: a dataset with the spatial and temporal dimensions given in
                 the supplied parameters and no data variables
        """
        bbox = open_params['bbox']
        spatial_res = open_params['spatial_res']
        # arange returns a half-open range, so we add *almost* a whole
        # spatial_res to the upper limit to make sure that it's included.
        lons = np.arange(bbox[0], bbox[2] + (spatial_res * 0.99), spatial_res)
        lats = np.arange(bbox[1], bbox[3] + (spatial_res * 0.99), spatial_res)

        time_range = open_params['time_range']
        times = self._create_time_range(time_range[0], time_range[1],
                                        open_params['time_period'])
        return xr.Dataset({}, coords={'time': times, 'lat': lats, 'lon': lons})

    @staticmethod
    def _create_time_range(t_start: str, t_end: str, t_interval: str):
        """Turn a start, end, and time interval into an array of datetime64s

        The array will contain times spaced at t_interval.
        If the time from start to end is not an exact multiple of the
        specified interval, the range will extend beyond t_end by a fraction
        of an interval.

        :param t_start: start of time range (inclusive) (ISO 8601)
        :param t_end: end of time range (inclusive) (ISO 8601)
        :param t_interval: time interval (format e.g. "2W", "3M" "1Y")
        :return: a NumPy array of datetime64 data from t_start to t_end with
                 an interval of t_period. If t_period is in months or years,
                 t_start and t_end will be rounded (down and up respectively)
                 to the nearest whole month.
        """
        dt_start = dateutil.parser.isoparse(t_start)
        dt_end = datetime.datetime.now() if t_end is None \
            else dateutil.parser.isoparse(t_end)
        period_number, period_unit = CDSDataOpener._parse_time_period(t_interval)
        timedelta = np.timedelta64(period_number, period_unit)
        relativedelta = CDSDataOpener._period_to_relativedelta(period_number,
                                                               period_unit)
        one_microsecond = dateutil.relativedelta.relativedelta(microseconds=1)
        # Months and years can be of variable length, so we need to reduce the
        # resolution of the start and end appropriately if the aggregation
        # period is in one of these units.
        if period_unit in 'MY':
            range_start = dt_start.strftime('%Y-%m')
            range_end = (dt_end + relativedelta - one_microsecond). \
                strftime('%Y-%m')
        else:
            range_start = dt_start.isoformat()
            range_end = (dt_end + relativedelta - one_microsecond).isoformat()

        return np.arange(range_start, range_end, timedelta,
                         dtype=f'datetime64')

    @staticmethod
    def _parse_time_period(specifier: str) -> Tuple[int, str]:
        """Convert a time period (e.g. '10D', 'Y') to a NumPy timedelta"""
        time_match = re.match(r'^(\d+)([hmsDWMY])$',
                              specifier)
        time_number_str = time_match.group(1)
        time_number = 1 if time_number_str == '' else int(time_number_str)
        time_unit = time_match.group(2)
        return time_number, time_unit

    @staticmethod
    def _period_to_relativedelta(number: int, unit: str) \
            -> dateutil.relativedelta:
        conversion = dict(Y='years', M='months', D='days', W='weeks',
                          h='hours', m='minutes', s='seconds')
        return dateutil.relativedelta. \
            relativedelta(**{conversion[unit]: number})

    def _open_data_with_handler(self, handler, dataset_name, cds_api_params) \
            -> xr.Dataset:

        client = None
        try:
            client = cdsapi.Client()

            # We can't generate a safe unique filename (since the file is
            # created by client.retrieve, so name generation and file
            # creation won't be atomic). Instead we atomically create a
            # subdirectory of the temporary directory for the single file.
            subdir = tempfile.mkdtemp(dir=self._tempdir)
            file_path = os.path.join(subdir, 'data')

            # This call returns a Result object, which at present we make
            # no use of.
            client.retrieve(dataset_name, cds_api_params, file_path)
        finally:
            # The API doesn't close the session automatically, so we need to
            # do it explicitly here to avoid leaving an open socket.
            if client is not None:
                client.session.close()

        # TODO: Work out if/when/how to delete the subdirectory.
        # The whole temporary parent directory will be deleted when the
        # interpreter exits, but that could still allow a lot of files to
        # build up. xarray may read data lazily from the file, so we can't
        # just delete the file as soon as the handler reads it, and we don't
        # have control of the DataSet's lifecycle once we've returned it.
        # There is a close() method in DataSet which might come in handy here
        # -- we could wrap the DataSet in a decorator subclass with a close()
        # which carries out the deletion, or just monkeypatch the method in
        # the instance returned by handler.read_file.

        # Create a subdirectory within the temporary directory for use by the
        # dataset handler, if required. For instance, if the CDS API returns
        # an archive, the subdirectory may be used to hold the unpacked
        # contents. Note that we don't delete this temporary directory
        # ourselves (because it might be used for lazy reading of the ensuing
        # dataset once this method has returned) -- instead we rely on the
        # deletion of the parent temporary directory.
        temp_subdir = tempfile.mkdtemp(dir=self._tempdir)

        dataset = handler.read_file(dataset_name, cds_api_params, file_path,
                                    temp_subdir)
        return self._normalize_dataset(dataset)

    def _normalize_dataset(self, dataset):
        dataset = xcube.core.normalize.normalize_dataset(dataset)

        # These steps should be taken care of by the core normalizer now.
        # TODO: check that they are.
        # dataset = dataset.rename_dims({
        #     'longitude': 'lon',
        #     'latitude': 'lat'
        # })
        # dataset = dataset.rename_vars({'longitude': 'lon', 'latitude': 'lat'})
        # dataset.transpose('time', ..., 'lat', 'lon')

        dataset.coords['time'].attrs['standard_name'] = 'time'
        dataset.coords['lat'].attrs['standard_name'] = 'latitude'
        dataset.coords['lon'].attrs['standard_name'] = 'longitude'

        # Correct units not entirely clear: cubespec document says
        # degrees_north / degrees_east for WGS84 Schema, but SH Plugin
        # had decimal_degrees.
        dataset.coords['lat'].attrs['units'] = 'degrees_north'
        dataset.coords['lon'].attrs['units'] = 'degrees_east'

        # TODO: Temporal coordinate variables MUST have units, standard_name,
        # and any others. standard_name MUST be "time", units MUST have
        # format "<deltatime> since <datetime>", where datetime must have
        # ISO-format.

        if self._normalize_names:
            rename_dict = {}
            for name in dataset.data_vars.keys():
                normalized_name = re.sub(r'\W|^(?=\d)', '_', str(name))
                if name != normalized_name:
                    rename_dict[name] = normalized_name
            dataset_renamed = dataset.rename_vars(rename_dict)
            return dataset_renamed
        else:
            return dataset

    def _validate_data_id(self, data_id, allow_none=False):
        if (data_id is None) and allow_none:
            return
        if data_id not in self._handler_registry:
            raise ValueError(f'Unknown data id "{data_id}"')


class CDSDataStore(CDSDataOpener, DataStore):

    def __init__(self,
                 num_retries: Optional[int] = DEFAULT_NUM_RETRIES,
                 **kwargs):
        super().__init__(**kwargs)
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

    def get_data_ids(self, type_id: Optional[str] = None) -> \
            Iterator[Tuple[str, Optional[str]]]:
        self._assert_valid_type_id(type_id)
        return iter((data_id,
                     self._handler_registry[data_id].
                     get_human_readable_data_id(data_id))
                    for data_id in self._handler_registry)

    def has_data(self, data_id: str) -> bool:
        return data_id in self._handler_registry

    def describe_data(self, data_id: str) -> DataDescriptor:
        self._validate_data_id(data_id)
        return self._handler_registry[data_id].describe_data(data_id)

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
