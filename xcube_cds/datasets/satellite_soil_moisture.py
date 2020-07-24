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

    def transform_params(self, plugin_params, data_id: str) -> \
            Tuple[str, Dict[str, Any]]:

        # We don't need to check the argument format, since CDSDataStore does
        # this for us. We can also ignore the dataset ID (constant) and
        # aggregation type (only needed for describe_data).
        _, variable_spec, _ = data_id.split(':')

        variable = dict(
            saturation='soil_moisture_saturation',
            volumetric='volumetric_surface_soil_moisture')[variable_spec]

        time_aggregation = {
            '1D': 'day_average',
            '10D': '10_day_average',
            '1M': 'month_average'}[plugin_params['time_period']]

        # TODO: Allow selection of passive sensor
        # At the moment the sensor type is entirely determined by the variable,
        # but in fact two possibilities are possible for 'volumetric':
        # 'combined_passive_and_active' and 'passive'.
        type_of_sensor = \
            dict(saturation='active',
                 volumetric='combined_passive_and_active')[variable_spec]

        cds_params = dict(
            variable=variable,
            type_of_sensor=type_of_sensor,
            time_aggregation=time_aggregation,
            type_of_record='cdr',
            version='v201912.0.0',
            format='tgz'
        )

        time_selectors = self.transform_time_params(
            self.convert_time_range(plugin_params['time_range']))

        cds_params.update(time_selectors)
        return 'satellite-soil-moisture', cds_params

    def read_file(self, dataset_name: str, cds_api_params: Dict,
                  file_path: str, temp_dir: str):
        # Create another directory within the temporary directory to hold
        # the contents of the .tar.gz file. Note that we don't delete this
        # temporary directory ourselves, instead relying on the deletion of
        # the parent temporary directory.
        temp_subdir = tempfile.mkdtemp(dir=temp_dir)

        # Unpack the .tar.gz into the temporary subdirectory.
        with tarfile.open(file_path) as tgz_file:
            tgz_file.extractall(path=temp_subdir)

        paths = [os.path.join(temp_subdir, filename) for filename in
                 next(os.walk(temp_subdir))[2]]

        # I'm not sure if xr.open_mfdataset calls through to
        # netCDF4.MFDataset. If it does, note that the latter supports
        # NetCDF4 Classic, but *not* full NetCDF4 -- however, in this case
        # it's OK because the Product User Guide (C3S_312a_Lot7_EODC_2016SC1,
        # §1, p. 12) states that the data are in Classic format,
        # and inspection of some downloaded files confirms it.
        return xr.open_mfdataset(paths, combine='by_coords')

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

    def get_supported_data_ids(self) -> List[str]:
        return list(self._data_id_map)

    def get_open_data_params_schema(self, data_id: Optional[str] = None) -> \
            JsonObjectSchema:
        # TODO: Adapt the schema according to the dataset suffixes.
        params = dict(
            dataset_name=JsonStringSchema(min_length=1,
                                          enum=self.get_supported_data_ids()),
            variable_names=JsonArraySchema(
                items=(JsonStringSchema(
                    min_length=1,
                    enum=['soil_moisture_saturation',
                          'volumetric_surface_soil_moisture'])),
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
        # TODO: Work out how to deal with different data formats.
        # The output data format is dependent on the request parameters, so
        # it is impossible to give a full and correct description from only
        # the data_id. Without changing the xcube store API, the only way
        # around this would be to encode the relevant parameters into the
        # data_id itself.
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
