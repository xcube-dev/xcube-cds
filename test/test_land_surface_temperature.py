import unittest

from xcube_cds.datasets.land_surface_temperature import LandSurfaceTemperatureDatasetHandler


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

