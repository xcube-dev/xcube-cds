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
from typing import List, Optional

from xcube.core.store import DataDescriptor, DatasetDescriptor, \
    VariableDescriptor
from xcube.util.jsonschema import JsonObjectSchema, JsonStringSchema, \
    JsonArraySchema, JsonNumberSchema

from xcube_cds.store import CDSDatasetHandler


class SoilMoistureHandler(CDSDatasetHandler):

    def transform_params(self, plugin_params, data_id: str):
        raise NotImplementedError()

    def read_file(self, dataset_name, cds_api_params, file_path):
        raise NotImplementedError()

    def __init__(self):
        self._data_id_map = \
            {'satellite-soil-moisture': 'Satellite soil moisture'}

    def get_supported_data_ids(self) -> List[str]:
        return list(self._data_id_map)

    def get_open_data_params_schema(self, data_id: Optional[str] = None) -> \
            JsonObjectSchema:
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
