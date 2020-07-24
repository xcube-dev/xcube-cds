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

import unittest

from jsonschema import ValidationError

from xcube_cds.store import CDSDataOpener
from xcube_cds.store import CDSDataStore
import xcube


class CDSStoreTest(unittest.TestCase):

    def test_open(self):
        opener = CDSDataOpener()
        dataset = opener.open_data(
            'reanalysis-era5-single-levels-monthly-means:'
            'monthly_averaged_reanalysis',
            variable_names=['2m_temperature'],
            bbox=[-1, -1, 1, 1],
            spatial_res=0.25,
            time_period='1M',
            time_range=['2015-10-15', '2016-02-02']
        )
        self.assertIsNotNone(dataset)
        # We expect the closest representable time selection corresponding
        # to the requested range: months 10-12 and 1-2 for years 2015 and 2016,
        # thus (3 + 2) * 2 = 10 time-points in total.
        self.assertEqual(10, len(dataset.variables['time']))

    def test_normalize_variable_names(self):
        store = CDSDataStore(normalize_names=True)
        dataset = store.open_data(
            'reanalysis-era5-single-levels-monthly-means:'
            'monthly_averaged_reanalysis',
            # Should be returned as p54.162, and normalized to p54_162.
            variable_names=['vertical_integral_of_temperature'],
            bbox=[-2, -2, 2, 2],
            spatial_res=1.0,
            time_range=['2019-01-01', None]
        )
        self.assertIsNotNone(dataset)
        self.assertTrue('p54_162' in dataset.variables)

    def test_invalid_data_id(self):
        store = CDSDataStore()
        with self.assertRaises(ValueError):
            store.open_data(
                'this-data-id-does-not-exist',
                variable_names=['2m_temperature'],
                hours=[0], months=[1], years=[2019]
            )

    def test_request_parameter_out_of_range(self):
        store = CDSDataStore()
        with self.assertRaises(ValidationError):
            store.open_data(
                'reanalysis-era5-single-levels-monthly-means:'
                'monthly_averaged_reanalysis_by_hour_of_day',
                variable_names=['2m_temperature'],
                bbox=[-1, -1, 181, 1],
                spatial_res=0.25,
                time_period='1M',
                time_range=['2019-01-01', '2020-12-31']
            )

    def test_era5_land_monthly(self):
        store = CDSDataStore()
        dataset = store.open_data(
            'reanalysis-era5-land-monthly-means:'
            'monthly_averaged_reanalysis',
            variable_names=['2m_temperature', '10m_u_component_of_wind'],
            bbox=[9.5, 49.5, 10.5, 50.5],
            spatial_res=0.1,
            time_period='1M',
            time_range=['2015-01-01', '2016-12-31']
        )
        self.assertIsNotNone(dataset)
        self.assertTrue('t2m' in dataset.variables)
        self.assertTrue('u10' in dataset.variables)

    def test_era5_single_levels_hourly(self):
        store = CDSDataStore()
        dataset = store.open_data(
            'reanalysis-era5-single-levels:'
            'reanalysis',
            variable_names=['2m_temperature'],
            bbox=[9, 49, 11, 51],
            spatial_res=0.25,
            time_period='1H',
            time_range=['2015-01-01 20:00',
                        '2015-01-02 08:00']
        )
        self.assertIsNotNone(dataset)
        self.assertTrue('t2m' in dataset.variables)
        self.assertEqual(26, len(dataset.variables['time']))

    def test_era5_land_hourly(self):
        store = CDSDataStore()
        dataset = store.open_data(
            'reanalysis-era5-land',
            variable_names=['2m_temperature'],
            bbox=[9.5, 49.5, 10.5, 50.5],
            spatial_res=0.1,
            time_period='1H',
            time_range=['2015-01-01 20:00',
                        '2015-01-02 08:00']
        )
        self.assertIsNotNone(dataset)
        self.assertTrue('t2m' in dataset.variables)
        self.assertEqual(26, len(dataset.variables['time']))

    def test_era5_bounds(self):
        opener = CDSDataOpener()
        dataset = opener.open_data(
            'reanalysis-era5-single-levels-monthly-means:'
            'monthly_averaged_reanalysis',
            variable_names=['2m_temperature'],
            bbox=[-180, -90, 180, 90],
            spatial_res=0.25,
            time_period='1M',
            time_range=['2015-10-15', '2015-10-15']
        )

        self.assertIsNotNone(dataset)

        west, south, east, north = xcube.core.geom.get_dataset_bounds(dataset)
        self.assertGreaterEqual(west, -180.0)
        self.assertGreaterEqual(south, -90.0)
        self.assertLessEqual(east, 180.0)
        self.assertLessEqual(north, 90.0)
        self.assertNotEqual(west, east)
        self.assertLessEqual(south, north)

    def test_soil_moisture(self):
        opener = CDSDataOpener()
        dataset = opener.open_data(
            'satellite-soil-moisture:volumetric:aggregated',
            variable_names=['volumetric_surface_soil_moisture'],
            bbox=[-180, -90, 180, 90],
            spatial_res=0.25,
            time_period='1M',
            time_range=['2015-01-01', '2015-02-28']
        )
        self.assertTrue('sm' in dataset.variables)
        self.assertEqual(2, len(dataset.variables['time']))


if __name__ == '__main__':
    unittest.main()
