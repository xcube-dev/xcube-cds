import unittest

from test.mocks import get_cds_client
from xcube_cds.datasets.land_surface_temperature import LandSurfaceTemperatureDatasetHandler
from xcube_cds.store import CDSDataOpener


class LandSurfaceTemperatureDatasetHandlerTest(unittest.TestCase):

    def setUp(self):
        self._lst_handler = LandSurfaceTemperatureDatasetHandler()

    def test_get_supported_data_ids(self):
        data_ids = self._lst_handler.get_supported_data_ids()

        self.assertEqual(1, len(data_ids))
        self.assertEqual("satellite-land-surface-temperature", data_ids[0])

    def test_get_open_data_params_schema(self):
        open_schema = self._lst_handler.get_open_data_params_schema("satellite-land-surface-temperature")
        self.assertIn("bbox", open_schema.properties)
        self.assertIn("time_range", open_schema.properties)
        self.assertIn("observation_time", open_schema.properties)
        self.assertNotIn("variable_names", open_schema.properties)
        self.assertNotIn("spatial_res", open_schema.properties)
        self.assertCountEqual(["time_range"], open_schema.required)

    def test_get_faulty_human_readable_data_id(self):
        with self.assertRaises(ValueError) as ve:
            self._lst_handler.get_human_readable_data_id("satellite-land-surface-temprature")
        self.assertEqual(
            "Data id 'satellite-land-surface-temprature' not provided by Land Surface Temperature Handler",
            str(ve.exception)
        )

    def test_get_human_readable_data_id(self):
        human_readable_data_id = self._lst_handler.get_human_readable_data_id("satellite-land-surface-temperature")
        self.assertEqual(
            "Land surface temperature monthly gridded data from 1995 to present derived from satellite observations",
            human_readable_data_id
        )

    def test_describe_data(self):
        descriptor = self._lst_handler.describe_data("satellite-land-surface-temperature")
        self.assertEqual("satellite-land-surface-temperature", descriptor.data_id)
        self.assertEqual("WGS84", descriptor.crs)
        self.assertEqual((-180.0, -90.0, 180.0, 90.0), descriptor.bbox)
        self.assertEqual(0.01, descriptor.spatial_res)
        self.assertEqual("1995-06-01", descriptor.time_range[0])
        self.assertEqual("2025-06-30", descriptor.time_range[1])
        self.assertEqual("1M", descriptor.time_period)
        self.assertEqual(
            {'dtime', 'lcc', 'lst', 'lst_unc_loc_atm', 'lst_unc_loc_cor', 'lst_unc_loc_sfc', 'lst_unc_ran',
             'lst_unc_sys', 'lst_uncertainty', 'n', 'sataz', 'satze', 'solaz', 'solze'},
            set(descriptor.data_vars.keys())
        )
        self.assertEqual(
            ("time", "lat", "lon"), descriptor.data_vars["lst"].dims
        )

    def test_open_data(self):
        opener = CDSDataOpener(
            client_class=get_cds_client(),
            endpoint_url="dummy",
            cds_api_key="dummy",
        )
        dataset = opener.open_data(
            "satellite-land-surface-temperature",
            bbox=[-1, -1, 1, 1],
            time_range=["2015-02-01", "2015-03-31"],
            observation_time=["day", "night"]
        )
        self.assertIsNotNone(dataset)
        self.assertEqual(
            [2, 200, 200],
            [dataset.sizes["time"], dataset.sizes["lat"], dataset.sizes["lon"]],
        )
        self.assertEqual(
            {'dtime', 'lcc', 'lst', 'lst_unc_loc_atm', 'lst_unc_loc_cor', 'lst_unc_loc_sfc', 'lst_unc_ran',
             'lst_unc_sys', 'lst_uncertainty', 'n', 'sataz', 'satze', 'solaz', 'solze'},
            set(dataset.data_vars.keys())
        )
