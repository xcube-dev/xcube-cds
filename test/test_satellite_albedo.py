# MIT License
#
# Copyright (c) 2026 Brockmann Consult GmbH
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

from test.mocks import get_cds_client
from xcube_cds.datasets.satellite_albedo import AlbedoHandler
from xcube_cds.store import CDSDataOpener

_CDS_API_URL = "dummy"
_CDS_API_KEY = "dummy"


class AlbedoHandlerTest(unittest.TestCase):

    def setUp(self):
        self._handler = AlbedoHandler()

    def test_get_supported_data_ids(self):
        data_ids = self._handler.get_supported_data_ids()

        self.assertEqual(12, len(data_ids))
        self.assertEqual(
            {
                "satellite-albedo:avhrr:albb_bh",
                "satellite-albedo:avhrr:albb_dh",
                "satellite-albedo:avhrr:alsp_bh",
                "satellite-albedo:avhrr:alsp_dh",
                "satellite-albedo:vgt:albb_bh",
                "satellite-albedo:vgt:albb_dh",
                "satellite-albedo:vgt:alsp_bh",
                "satellite-albedo:vgt:alsp_dh",
                "satellite-albedo:olci_and_slstr:albb_bh",
                "satellite-albedo:olci_and_slstr:albb_dh",
                "satellite-albedo:olci_and_slstr:alsp_bh",
                "satellite-albedo:olci_and_slstr:alsp_dh",
            },
            set(data_ids),
        )

    def test_get_open_data_params_schema(self):
        open_schema = self._handler.get_open_data_params_schema(
            "satellite-albedo:avhrr:albb_bh"
        )
        self.assertIn("bbox", open_schema.properties)
        self.assertIn("time_range", open_schema.properties)
        self.assertIn("satellites", open_schema.properties)
        self.assertIn("product_versions", open_schema.properties)
        self.assertNotIn("spatial_res", open_schema.properties)
        self.assertCountEqual(["time_range"], open_schema.required)

    def test_get_human_readable_data_id_false(self):
        with self.assertRaises(ValueError) as ve:
            _ = self._handler.get_human_readable_data_id("satellite-albedo")
        self.assertEqual(
            "Data id 'satellite-albedo' not provided by Albedo Handler. "
            "Supported data ids: satellite-albedo:avhrr:albb_bh, satellite-albedo:vgt:albb_bh, "
            "satellite-albedo:olci_and_slstr:albb_bh, satellite-albedo:avhrr:albb_dh, "
            "satellite-albedo:vgt:albb_dh, satellite-albedo:olci_and_slstr:albb_dh, "
            "satellite-albedo:avhrr:alsp_bh, satellite-albedo:vgt:alsp_bh, "
            "satellite-albedo:olci_and_slstr:alsp_bh, satellite-albedo:avhrr:alsp_dh, "
            "satellite-albedo:vgt:alsp_dh, satellite-albedo:olci_and_slstr:alsp_dh.",
            str(ve.exception),
        )

    def test_get_human_readable_data_id_correct(self):
        human_readable_data_id = self._handler.get_human_readable_data_id(
            "satellite-albedo:avhrr:albb_bh"
        )
        self.assertEqual(
            human_readable_data_id,
            "Surface albedo 10-daily gridded data from 1981 to present (AVHRR Broadband Hemispherical)",
        )

    def test_describe_data_avhrr_albb_bh(self):
        descriptor = self._handler.describe_data(
            "satellite-albedo:avhrr:albb_bh"
        )
        avhrr_albb_bh_vars = {
            "AL_BH_BB",
            "AL_BH_BB_ERR",
            "AL_BH_NI",
            "AL_BH_NI_ERR",
            "AL_BH_VI",
            "AL_BH_VI_ERR",
            "AGE",
            "NMOD",
            "QFLAG",
        }
        self.assertEqual(avhrr_albb_bh_vars, set(descriptor.data_vars.keys()))

    def test_describe_data_avhrr_albb_dh(self):
        descriptor = self._handler.describe_data(
            "satellite-albedo:avhrr:albb_dh"
        )
        avhrr_albb_dh_vars = {
            "AL_DH_BB",
            "AL_DH_BB_ERR",
            "AL_DH_NI",
            "AL_DH_NI_ERR",
            "AL_DH_VI",
            "AL_DH_VI_ERR",
            "AGE",
            "NMOD",
            "QFLAG",
        }
        self.assertEqual(avhrr_albb_dh_vars, set(descriptor.data_vars.keys()))

    def test_describe_data_avhrr_alsp_bh(self):
        descriptor = self._handler.describe_data(
            "satellite-albedo:avhrr:alsp_bh"
        )
        avhrr_alsp_bh_vars = {
            "AL_BH_B0",
            "AL_BH_B0_ERR",
            "AL_BH_B2",
            "AL_BH_B2_ERR",
            "AL_BH_B3",
            "AL_BH_B3_ERR",
            "AL_BH_MIR",
            "AL_BH_MIR_ERR",
            "AGE",
            "NMOD",
            "QFLAG",
        }
        self.assertEqual(avhrr_alsp_bh_vars, set(descriptor.data_vars.keys()))

    def test_describe_data_avhrr_alsp_dh(self):
        descriptor = self._handler.describe_data(
            "satellite-albedo:avhrr:alsp_dh"
        )
        avhrr_alsp_dh_vars = {
            "AL_DH_B0",
            "AL_DH_B0_ERR",
            "AL_DH_B2",
            "AL_DH_B2_ERR",
            "AL_DH_B3",
            "AL_DH_B3_ERR",
            "AL_DH_MIR",
            "AL_DH_MIR_ERR",
            "AGE",
            "NMOD",
            "QFLAG",
        }
        self.assertEqual(avhrr_alsp_dh_vars, set(descriptor.data_vars.keys()))

    def test_describe_data_vgt_albb_bh(self):
        descriptor = self._handler.describe_data(
            "satellite-albedo:vgt:albb_bh"
        )
        vgt_albb_bh_vars = {
            "AL_BH_BB",
            "AL_BH_BB_ERR",
            "AL_BH_NI",
            "AL_BH_NI_ERR",
            "AL_BH_VI",
            "AL_BH_VI_ERR",
            "AGE",
            "NMOD",
            "QFLAG",
        }
        self.assertEqual(vgt_albb_bh_vars, set(descriptor.data_vars.keys()))

    def test_describe_data_vgt_albb_dh(self):
        descriptor = self._handler.describe_data(
            "satellite-albedo:vgt:albb_dh"
        )
        vgt_albb_dh_vars = {
            "AL_DH_BB",
            "AL_DH_BB_ERR",
            "AL_DH_NI",
            "AL_DH_NI_ERR",
            "AL_DH_VI",
            "AL_DH_VI_ERR",
            "AGE",
            "NMOD",
            "QFLAG",
        }
        self.assertEqual(vgt_albb_dh_vars, set(descriptor.data_vars.keys()))

    def test_describe_data_vgt_alsp_bh(self):
        descriptor = self._handler.describe_data(
            "satellite-albedo:vgt:alsp_bh"
        )
        vgt_alsp_bh_vars = {
            "AL_BH_B0",
            "AL_BH_B0_ERR",
            "AL_BH_B2",
            "AL_BH_B2_ERR",
            "AL_BH_B3",
            "AL_BH_B3_ERR",
            "AL_BH_MIR",
            "AL_BH_MIR_ERR",
            "AGE",
            "NMOD",
            "QFLAG",
        }
        self.assertEqual(vgt_alsp_bh_vars, set(descriptor.data_vars.keys()))

    def test_describe_data_vgt_alsp_dh(self):
        descriptor = self._handler.describe_data(
            "satellite-albedo:vgt:alsp_dh"
        )
        vgt_alsp_dh_vars = {
            "AL_DH_B0",
            "AL_DH_B0_ERR",
            "AL_DH_B2",
            "AL_DH_B2_ERR",
            "AL_DH_B3",
            "AL_DH_B3_ERR",
            "AL_DH_MIR",
            "AL_DH_MIR_ERR",
            "AGE",
            "NMOD",
            "QFLAG",
        }
        self.assertEqual(vgt_alsp_dh_vars, set(descriptor.data_vars.keys()))

    def test_describe_data_s3_albb_bh(self):
        descriptor = self._handler.describe_data(
            "satellite-albedo:olci_and_slstr:albb_bh"
        )
        s3_albb_bh_vars = {
            "AL_BH_BB",
            "AL_BH_BB_ERR",
            "AL_BH_NI",
            "AL_BH_NI_ERR",
            "AL_BH_VI",
            "AL_BH_VI_ERR",
            "QFLAG",
        }
        self.assertEqual(s3_albb_bh_vars, set(descriptor.data_vars.keys()))

    def test_describe_data_s3_albb_dh(self):
        descriptor = self._handler.describe_data(
            "satellite-albedo:olci_and_slstr:albb_dh"
        )
        s3_albb_dh_vars = {
            "AL_DH_BB",
            "AL_DH_BB_ERR",
            "AL_DH_NI",
            "AL_DH_NI_ERR",
            "AL_DH_VI",
            "AL_DH_VI_ERR",
            "QFLAG",
        }
        self.assertEqual(s3_albb_dh_vars, set(descriptor.data_vars.keys()))

    def test_describe_data_s3_alsp_bh(self):
        descriptor = self._handler.describe_data(
            "satellite-albedo:olci_and_slstr:alsp_bh"
        )
        s3_alsp_bh_vars = {
            "AL_BH_Oa03",
            "AL_BH_Oa03_ERR",
            "AL_BH_Oa04",
            "AL_BH_Oa04_ERR",
            "AL_BH_Oa07",
            "AL_BH_Oa07_ERR",
            "AL_BH_Oa17",
            "AL_BH_Oa17_ERR",
            "AL_BH_Oa21",
            "AL_BH_Oa21_ERR",
            "AL_BH_S1",
            "AL_BH_S1_ERR",
            "AL_BH_S2",
            "AL_BH_S2_ERR",
            "AL_BH_S5",
            "AL_BH_S5_ERR",
            "AL_BH_S6",
            "AL_BH_S6_ERR",
            "QFLAG",
        }
        self.assertEqual(s3_alsp_bh_vars, set(descriptor.data_vars.keys()))

    def test_describe_data_s3_alsp_dh(self):
        descriptor = self._handler.describe_data(
            "satellite-albedo:olci_and_slstr:alsp_dh"
        )
        s3_alsp_dh_vars = {
            "AL_DH_Oa03",
            "AL_DH_Oa03_ERR",
            "AL_DH_Oa04",
            "AL_DH_Oa04_ERR",
            "AL_DH_Oa07",
            "AL_DH_Oa07_ERR",
            "AL_DH_Oa17",
            "AL_DH_Oa17_ERR",
            "AL_DH_Oa21",
            "AL_DH_Oa21_ERR",
            "AL_DH_S1",
            "AL_DH_S1_ERR",
            "AL_DH_S2",
            "AL_DH_S2_ERR",
            "AL_DH_S5",
            "AL_DH_S5_ERR",
            "AL_DH_S6",
            "AL_DH_S6_ERR",
            "QFLAG",
        }
        self.assertEqual(s3_alsp_dh_vars, set(descriptor.data_vars.keys()))

    def test_open_data(self):
        opener = CDSDataOpener(
            client_class=get_cds_client(),
            endpoint_url=_CDS_API_URL,
            cds_api_key=_CDS_API_KEY,
        )
        dataset = opener.open_data(
            "satellite-albedo:vgt:albb_dh",
            bbox=[-1, -1, 1, 1],
            time_range=["2009-01-01", "2009-02-28"],
        )
        self.assertIsNotNone(dataset)
        self.assertEqual(
            [6, 224, 224],
            [
                dataset.sizes["time"],
                dataset.sizes["lat"],
                dataset.sizes["lon"],
            ],
        )
        self.assertCountEqual(
            [
                "crs",
                "AL_DH_BB",
                "AL_DH_BB_ERR",
                "AL_DH_NI",
                "AL_DH_NI_ERR",
                "AL_DH_VI",
                "AL_DH_VI_ERR",
                "AGE",
                "NMOD",
                "QFLAG",
            ],
            dataset.data_vars,
        )
