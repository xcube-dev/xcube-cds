# MIT License
#
# Copyright (c) 2020-2021 Brockmann Consult GmbH
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

import collections
import os
import tarfile
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

import xarray as xr
from xcube.core.store import DatasetDescriptor
from xcube.core.store import VariableDescriptor
from xcube.util.jsonschema import JsonArraySchema
from xcube.util.jsonschema import JsonDateSchema
from xcube.util.jsonschema import JsonObjectSchema
from xcube.util.jsonschema import JsonStringSchema

from xcube_cds.store import CDSDatasetHandler

VariableProperties = collections.namedtuple(
    'VariableProperties',
    ['cdr_types', 'start_date', 'end_date']
)


class SeaiceHandler(CDSDatasetHandler):

    _data_id_map = {
        'satellite-sea-ice-thickness:envisat':
            'Sea ice thickness (Envisat)',
        'satellite-sea-ice-thickness:cryosat-2':
            'Sea ice thickness (CryoSat-2)',
    }

    # Map second component of data ID to variable and sensor type information
    _var_map = {
        'envisat':
            VariableProperties(['cdr'],
                               ['2002-10-01'], ['2010-10-31']),
        'cryosat-2':
            VariableProperties(['cdr', 'icdr'],
                               ['2010-11-01'], None)
    }

    def get_supported_data_ids(self) -> List[str]:
        return list(self._data_id_map)

    def get_open_data_params_schema(self, data_id: str) -> \
            JsonObjectSchema:
        _, mission_spec, _ = data_id.split(':')
        variable_properties = self._var_map[mission_spec]

        params = dict(
            time_range=JsonDateSchema.new_range(
                min_date=variable_properties.start_date,
                max_date=variable_properties.end_date
            ),
            # crs, bbox, time_period, and spatial_res omitted
            # since they're constant.

            # type_of_sensor will be added below *only* if >1 type available.

            # There's only one variable available, but we can't
            # omit variable_names, because we need to support the
            # variable_names=[] case (to produce an empty cube).
            # It has been announced that in future versions the selection of
            # individual variables will be supported
            variable_names=JsonArraySchema(
                items=(JsonStringSchema(
                    min_length=0,
                    enum=['all'],
                    default='all')),
                unique_items=True,
                default=['all']
            ),
            type_of_record=JsonStringSchema(
                enum=variable_properties.cdr_types,
                title='Type of record',
                description=(
                    'This dataset combines a Climate Data Record (CDR), '
                    'which has sufficient length, consistency, and continuity '
                    'to be used to assess climate variability and change, '
                    'and an Interim Climate Data Record (ICDR), which provides '
                    'regular temporal extensions to the CDR and where '
                    'consistency with the CDR is expected but not extensively '
                    'checked. Here, the CDR is based on measurements from the '
                    'RA-2 altimeter on Envisat (October 2002 to October 2010) '
                    'and the SIRAL altimeter on CryoSat-2 '
                    '(November 2010 to April 2020). The ICDR is based on '
                    'observations from CryoSat-2 only (from April 2015 onward) '
                    'and is updated monthly with a one-month delay behind '
                    'real time. Users should note that the quality and '
                    'accuracy of the data record are higher during '
                    'the CryoSat-2 period than during the Envisat period. '
                    'As a result, care should be taken when combining '
                    'the two missions to assess long-term changes and trends. '
                    'More information can be found in the Product User Guide '
                    'and Product Quality Assessment Report.'),
                default='cdr'),
            version=JsonStringSchema(
                enum=['2.0', '1.0'],
                title='Data version',
                default='2.0'),
        )

        return JsonObjectSchema(
            properties=params,
            required=['time_range'],
            additional_properties=False
        )

    def get_human_readable_data_id(self, data_id: str) -> str:
        return self._data_id_map[data_id]

    def transform_params(self, opener_params: Dict, data_id: str) -> \
            Tuple[str, Dict[str, Any]]:
        # We don't need to check the argument format, since CDSDataStore does
        # this for us. We can also ignore the dataset ID (constant).
        _, mission = data_id.split(':')
        variable_properties = self._var_map[mission]

        # Version string needs to be modified slightly
        version = opener_params.get('version', '2_0').replace('.', '_')

        # If no climate data record type is specified in the opener parameters,
        # it is determined from the data ID
        cdr_type = opener_params.get(
            'type_of_record',
            variable_properties.cdr_types[0]
        )

        cds_params = dict(
            satellite=mission,
            cdr_type=cdr_type,
            type_of_record=cdr_type,
            version=version,
            variable='all',
            format='tgz'
        )

        time_selectors = self.transform_time_params(
            self.convert_time_range(opener_params['time_range']))
        time_selectors.pop('time', None)
        time_selectors.pop('day', None)
        unsupported_months = ['05', '06', '07', '08', '09']
        for unsupported_month in unsupported_months:
            if unsupported_month in time_selectors['month']:
                time_selectors['month'].remove(unsupported_month)
        cds_params.update(time_selectors)

        # Transform singleton list values into their single members, as
        # required by the CDS API.
        unwrapped = self.unwrap_singleton_values(cds_params)

        return 'satellite-sea-ice-thickness', unwrapped

    def read_file(self, dataset_name: str, open_params: Dict,
                  cds_api_params: Dict, file_path: str, temp_dir: str):
        # todo as this is identical to code from satellite_soil_moisture:
        # Consider refactor to common method read_tar_gz

        # Unpack the .tar.gz into the temporary directory.
        with tarfile.open(file_path) as tgz_file:
            tgz_file.extractall(path=temp_dir)

        paths = [os.path.join(temp_dir, filename) for filename in
                 next(os.walk(temp_dir))[2]]

        ds = xr.open_mfdataset(paths, combine="by_coords",
                               engine="netcdf4", decode_cf=True)
        ds.attrs.update(self.combine_netcdf_time_limits(paths))

        return ds

    def describe_data(self, data_id: str) -> DatasetDescriptor:
        _, mission = data_id.split(':')

        variable_descriptors = [
            VariableDescriptor(
                name='sea_ice_thickness',
                dtype='float32',
                dims=('time', 'yc', 'xc'),
                attrs={'ancillary_variables': 'uncertainty '
                                              'status_flag '
                                              'quality_flag',
                       'comment': 'this field is the primary sea ice thickness '
                                  'estimate for this climate data record',
                       'coordinates': 'time lat lon',
                       'coverage_content_type': 'physicalMeasurement',
                       'grid_mapping': 'Lambert_Azimuthal_Grid',
                       'long_name': 'Sea Ice Thickness',
                       'standard_name': 'sea_ice_thickness',
                       'units': 'm'
                       }
            ),
            VariableDescriptor(
                name='quality_flag',
                dtype='int8',
                dims=('time', 'yc', 'xc'),
                attrs={
                    'comment': 'The expert assessment on retrieval quality is '
                               'only provided for grid cess with valid '
                               'thickness retrieval',
                    'coordinates': 'time lat lon',
                    'coverage_content_type': 'qualityInformation',
                    'flag_meanings': 'nominal_quality '
                                     'intermediate_quality '
                                     'low_quality '
                                     'no_data',
                    'flag_values': '0b 1b 2b 3b',
                    'grid_mapping': 'Lambert_Azimuthal_Grid',
                    'long_name': 'Sea Ice Thickness Quality Flag',
                    'standard_name': 'quality_flag',
                    'units': '1',
                    'valid_max': '3',
                    'valid_min': '0'
                }
            ),
            VariableDescriptor(
                name='status_flag',
                dtype='int8',
                dims=('time', 'yc', 'xc'),
                attrs={
                    'coordinates': 'time lat lon',
                    'coverage_content_type': 'qualityInformation',
                    'flag_meanings': 'nominal_retrieval '
                                     'no_data open_ocean '
                                     'satellite_pole_hole '
                                     'land_lake_landice '
                                     'retrieval_failed',
                    'flag_values': '0b 1b 2b 3b 4b 5b',
                    'grid_mapping': 'Lambert_Azimuthal_Grid',
                    'long_name': 'Sea Ice Thickness Status Flag',
                    'standard_name': 'status_flag',
                    'units': '1',
                    'valid_max': '5',
                    'valid_min': '0',
                }
            ),
            VariableDescriptor(
                name='uncertainty',
                dtype='float32',
                dims=('time', 'yc', 'xc'),
                attrs={
                    'coordinates': 'time lat lon',
                    'coverage_content_type': 'auxiliaryInformation',
                    'grid_mapping': 'Lambert_Azimuthal_Grid',
                    'long_name': 'Sea Ice Thickness Uncertainty',
                    'standard_name': 'sea_ice_thickness standard_error',
                    'units': 'm'
                }
            )
        ]
        coordinate_descriptors = [
            VariableDescriptor(
                name='lat',
                dtype='float64',
                dims=('yc', 'xc'),
                attrs={
                    'coverage_content_type': 'coordinate',
                    'long_name': 'latitude coordinate',
                    'standard_name': 'latitude',
                    'units': 'degrees_north'
                }
            ),
            VariableDescriptor(
                name='lon',
                dtype='float64',
                dims=('yc', 'xc'),
                attrs={
                    'coverage_content_type': 'coordinate',
                    'long_name': 'longitude coordinate',
                    'standard_name': 'longitude',
                    'units': 'degrees_east'
                }
            ),
            VariableDescriptor(
                name='time',
                dtype='float64',
                dims='time',
                attrs={
                    'standard_name': 'time',
                    'units': 'seconds since 1970-01-01',
                    'long_name': 'Time',
                    'axis': 'T',
                    'calendar': 'standard',
                    'bounds': 'time_bnds',
                    'coverage_content_type': 'coordinate'
                }
            ),
            VariableDescriptor(
                name='time_bnds',
                dtype='float64',
                dims=('time', 'nv'),
                attrs={
                    'units': 'seconds since 1970-01-01',
                    'long_name': 'Time Bounds',
                    'coverage_content_type': 'coordinate'
                }
            ),
            VariableDescriptor(
                name='xc',
                dtype='float64',
                dims='xc',
                attrs={
                    'standard_name': 'projection_x_coordinate',
                    'units': 'km',
                    'long_name': 'x coordinate of projection (eastings)',
                    'coverage_content_type': 'coordinate',
                }
            ),
            VariableDescriptor(
                name='yc',
                dtype='float64',
                dims='yc',
                attrs={
                    'standard_name': 'projection_y_coordinate',
                    'units': 'km',
                    'long_name': 'y coordinate of projection (eastings)',
                    'coverage_content_type': 'coordinate',
                }
            ),
            VariableDescriptor(
                name='Lambert_Azimuthal_Grid',
                dtype='int8',
                dims=(),
                attrs={
                    'false_easting': 0.,
                    'false_northing': 0.,
                    'grid_mapping_name': 'lambert_azimuthal_equal_area',
                    'inverse_flattening': 298.257223563,
                    'latitude_of_projection_origin': 90.,
                    'longitude_of_projection_origin': 0.,
                    'proj4_string': '+proj=laea +lon_0=0 +datum=WGS84 '
                                    '+ellps=WGS84 +lat_0=90.0',
                    'semi_major_axis': 6378137.,
                }
            )
        ]

        return DatasetDescriptor(
            data_id=data_id,
            data_vars={desc.name: desc for desc in variable_descriptors},
            coords={desc.name: desc for desc in coordinate_descriptors},
            crs='EPSG:6931',
            bbox=(-180, 16.6239, 180, 90),
            spatial_res=25.0,
            time_range=('2002-10-01', None),
            open_params_schema=self.get_open_data_params_schema(data_id)
        )
