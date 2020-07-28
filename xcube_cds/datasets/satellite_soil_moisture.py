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
import tarfile
from typing import List, Optional, Dict, Tuple, Any
import xarray as xr
from xcube.core.store import DataDescriptor, DatasetDescriptor, \
    VariableDescriptor
from xcube.util.jsonschema import JsonObjectSchema, JsonStringSchema, \
    JsonArraySchema, JsonNumberSchema

from xcube_cds.store import CDSDatasetHandler


class SoilMoistureHandler(CDSDatasetHandler):

    def __init__(self):
        self._data_id_map = {
            'satellite-soil-moisture:saturation:daily':
                'Soil moisture (saturation, daily)',
            'satellite-soil-moisture:saturation:aggregated':
                'Soil moisture (saturation, aggregated)',
            'satellite-soil-moisture:volumetric:daily':
                'Soil moisture (volumetric, daily)',
            'satellite-soil-moisture:volumetric:aggregated':
                'Soil moisture (volumetric, aggregated)',
        }
        self._var_map = {
            'saturation': (['soil_moisture_saturation'], ['active']),
            'volumetric': (['volumetric_surface_soil_moisture'],
                           ['combined_passive_and_active', 'passive'])
        }

    def transform_params(self, opener_params, data_id: str) -> \
            Tuple[str, Dict[str, Any]]:

        # We don't need to check the argument format, since CDSDataStore does
        # this for us. We can also ignore the dataset ID (constant) and
        # aggregation type (only needed for describe_data).
        _, variable_spec, _ = data_id.split(':')

        variables = opener_params['variable_names']

        time_aggregation = {
            '1D': 'day_average',
            '10D': '10_day_average',
            '1M': 'month_average'}[opener_params['time_period']]

        cds_params = dict(
            variable=variables,
            type_of_sensor=opener_params['type_of_sensor'],
            time_aggregation=time_aggregation,
            type_of_record=opener_params['type_of_record'],
            version=opener_params['version'],
            format='tgz'
        )

        # TODO: check that this gets desingletonned before calling CDS

        time_selectors = self.transform_time_params(
            self.convert_time_range(opener_params['time_range']))

        cds_params.update(time_selectors)
        return 'satellite-soil-moisture', cds_params

    def read_file(self, dataset_name: str, cds_api_params: Dict,
                  file_path: str, temp_dir: str):

        # Unpack the .tar.gz into the temporary directory.
        with tarfile.open(file_path) as tgz_file:
            tgz_file.extractall(path=temp_dir)

        paths = [os.path.join(temp_dir, filename) for filename in
                 next(os.walk(temp_dir))[2]]

        # I'm not sure if xr.open_mfdataset calls through to
        # netCDF4.MFDataset. If it does, note that the latter supports
        # NetCDF4 Classic, but *not* full NetCDF4 -- however, in this case
        # it's OK because the Product User Guide (C3S_312a_Lot7_EODC_2016SC1,
        # §1, p. 12) states that the data are in Classic format,
        # and inspection of some downloaded files confirms it.
        ds = xr.open_mfdataset(paths, combine='by_coords')
        ds.attrs.update(self.combine_netcdf_time_limits(paths))
        return ds

    def get_supported_data_ids(self) -> List[str]:
        return list(self._data_id_map)

    def get_open_data_params_schema(self, data_id: str) -> JsonObjectSchema:
        _, variable_spec, aggregation = data_id.split(':')
        variables = self._var_map[variable_spec][0]
        sensors = self._var_map[variable_spec][1]
        params = dict(
            dataset_name=JsonStringSchema(min_length=1,
                                          enum=self.get_supported_data_ids()),
            # The only allowed variable is already determined by the
            # data_id, so this schema forces an array containing only that
            # variable.
            variable_names=JsonArraySchema(
                items=(JsonStringSchema(
                    min_length=1,
                    enum=variables,
                    default=variables[0])),
                unique_items=True
            ),
            # Source for CRS information: §6.5 of
            # https://www.esa-soilmoisture-cci.org/sites/default/files/documents/CCI2_Soil_Moisture_D3.3.1_Product_Users_Guide%201.2.pdf
            crs=JsonStringSchema(nullable=True, default='WGS84',
                                 enum=[None, 'WGS84']),
            # W, S, E, N (will be converted to N, W, S, E)
            bbox=JsonArraySchema(items=(
                JsonNumberSchema(minimum=-180, maximum=180),
                JsonNumberSchema(minimum=-90, maximum=90),
                JsonNumberSchema(minimum=-180, maximum=180),
                JsonNumberSchema(minimum=-90, maximum=90))),
            spatial_res=JsonNumberSchema(minimum=0.25,
                                         maximum=0.25,
                                         default=0.25),
            time_range=JsonArraySchema(
                items=[JsonStringSchema(format='date-time'),
                       JsonStringSchema(format='date-time', nullable=True)]),
            time_period=JsonStringSchema(enum=['1D', '10D', '1M']),
            # Non-standard parameters start here. There are complex
            # interdependencies between allowed values for these and for
            # the date specifiers, which can't be represented in JSON Schema.
            # The best we can do is to make them all available, set sensible
            # defaults, and trust that the user knows what they're requesting.
            type_of_sensor=JsonStringSchema(enum=sensors, default=sensors[0]),
            type_of_record=JsonStringSchema(enum=['cdr', 'icdr'],
                                            default='cdr'),
            version=JsonStringSchema(
                enum=['v201706.0.0', 'v201812.0.0', 'v201812.0.1',
                      'v201912.0.0'],
                default='v201912.0.0')

        )
        required = [
            'variable_names',
            'bbox',
            'spatial_res',
            'time_range',
        ]
        return JsonObjectSchema(
            properties=dict(
                **params,
            ),
            required=required
        )

    def get_human_readable_data_id(self, data_id: str):
        return self._data_id_map['data_id']

    def describe_data(self, data_id: str) -> DataDescriptor:
        # TODO: Adapt output to data_if suffixes
        return DatasetDescriptor(
            data_id=data_id,
            data_vars=[
                VariableDescriptor(
                    name='nobs',
                    dtype='int16',
                    dims=('time', 'lat', 'lon'),
                    attrs={'long_name': 'Number of valid observation'}
                ),
                VariableDescriptor(
                    name='sensor',
                    dtype='int16',
                    dims=('time', 'lat', 'lon'),
                    attrs={'long_name': 'Sensor'}
                ),
                VariableDescriptor(
                    name='freqbandID',
                    dtype='int16',
                    dims=('time', 'lat', 'lon'),
                    attrs={'long_name': 'Frequency Band Identification'}
                ),
                VariableDescriptor(
                    name='sm',
                    dtype='float32',
                    dims=('time', 'lat', 'lon'),
                    attrs={'units': 'm3 m-3',
                           'long_name': 'Volumetric Soil Moisture'}
                )
            ],
            crs='WGS84',
            bbox=(-180, -90, 180, 90),
            spatial_res=0.25,
            time_range=('1978-11-01', None),
            time_period='1M',
            open_params_schema=self.get_open_data_params_schema(data_id)
        )