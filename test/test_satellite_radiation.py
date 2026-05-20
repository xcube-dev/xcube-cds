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

import pytest

from xcube_cds.datasets.satellite_radiation import (
    SatelliteSurfaceRadiationBudgetHandler,
    _DATASET_NAME,
    _VARIABLES,
    _DATA_ID_MAP,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def handler():
    return SatelliteSurfaceRadiationBudgetHandler()


# ---------------------------------------------------------------------------
# get_data_ids
# ---------------------------------------------------------------------------

class TestGetDataIds:
    def test_returns_three_ids(self, handler):
        ids = list(handler.get_data_ids())
        assert len(ids) == 3

    def test_ids_have_correct_prefixes(self, handler):
        ids = [data_id for data_id, _ in handler.get_data_ids()]
        for suffix in _DATA_ID_MAP:
            assert f"{_DATASET_NAME}:{suffix}" in ids

    def test_attrs_are_none(self, handler):
        for _, attrs in handler.get_data_ids():
            assert attrs is None


# ---------------------------------------------------------------------------
# get_open_data_params_schema
# ---------------------------------------------------------------------------

class TestGetOpenDataParamsSchema:
    @pytest.mark.parametrize("suffix", list(_DATA_ID_MAP.keys()))
    def test_schema_is_dict(self, handler, suffix):
        data_id = f"{_DATASET_NAME}:{suffix}"
        schema = handler.get_open_data_params_schema(data_id)
        assert isinstance(schema, dict)
        assert schema["type"] == "object"

    def test_schema_contains_variable_names(self, handler):
        schema = handler.get_open_data_params_schema(
            f"{_DATASET_NAME}:clara"
        )
        props = schema["properties"]
        assert "variable_names" in props
        assert set(props["variable_names"]["items"]["enum"]) == set(
            _VARIABLES
        )

    def test_schema_contains_time_range(self, handler):
        schema = handler.get_open_data_params_schema(
            f"{_DATASET_NAME}:cci:esa"
        )
        assert "time_range" in schema["properties"]

    def test_schema_contains_bbox(self, handler):
        schema = handler.get_open_data_params_schema(
            f"{_DATASET_NAME}:cci:c3s"
        )
        assert "bbox" in schema["properties"]

    def test_unknown_data_id_raises(self, handler):
        with pytest.raises(ValueError, match="Unknown"):
            handler.get_open_data_params_schema("unknown-dataset:foo")


# ---------------------------------------------------------------------------
# get_data_source_info
# ---------------------------------------------------------------------------

class TestGetDataSourceInfo:
    def test_clara_info(self, handler):
        info = handler.get_data_source_info(f"{_DATASET_NAME}:clara")
        assert info["product_family"] == "clara_a3"
        assert info["origin"] == "eumetsat"
        assert info["cds_dataset_name"] == _DATASET_NAME

    def test_cci_esa_info(self, handler):
        info = handler.get_data_source_info(f"{_DATASET_NAME}:cci:esa")
        assert info["product_family"] == "cci"
        assert info["origin"] == "esa"

    def test_cci_c3s_info(self, handler):
        info = handler.get_data_source_info(f"{_DATASET_NAME}:cci:c3s")
        assert info["origin"] == "c3s"

    def test_time_aggregation_monthly(self, handler):
        for suffix in _DATA_ID_MAP:
            info = handler.get_data_source_info(f"{_DATASET_NAME}:{suffix}")
            assert info["time_aggregation"] == "monthly_mean"


# ---------------------------------------------------------------------------
# transform_params
# ---------------------------------------------------------------------------

class TestTransformParams:
    def test_basic_clara_request(self, handler):
        cds_name, request = handler.transform_params(
            opener_params={
                "variable_names": ["surface_incoming_shortwave_radiation"],
                "time_range": ["2010-01-01", "2010-03-31"],
            },
            data_id=f"{_DATASET_NAME}:clara",
        )
        assert cds_name == _DATASET_NAME
        assert request["product_family"] == "clara_a3"
        assert request["origin"] == "eumetsat"
        assert "surface_incoming_shortwave_radiation" in request["variable"]
        assert "2010" in request["year"]
        assert "01" in request["month"]
        assert "03" in request["month"]

    def test_cci_esa_request(self, handler):
        _, request = handler.transform_params(
            opener_params={"time_range": ["2005-06-01", "2005-08-31"]},
            data_id=f"{_DATASET_NAME}:cci:esa",
        )
        assert request["origin"] == "esa"
        assert request["product_family"] == "cci"

    def test_cci_c3s_request(self, handler):
        _, request = handler.transform_params(
            opener_params={"time_range": ["2020-01-01", "2020-01-31"]},
            data_id=f"{_DATASET_NAME}:cci:c3s",
        )
        assert request["origin"] == "c3s"

    def test_bbox_is_converted(self, handler):
        _, request = handler.transform_params(
            opener_params={
                "time_range": ["2015-01-01", "2015-01-31"],
                "bbox": [-10.0, 40.0, 30.0, 70.0],
            },
            data_id=f"{_DATASET_NAME}:clara",
        )
        # CDS area format: [north, west, south, east]
        assert request["area"] == [70.0, -10.0, 40.0, 30.0]

    def test_no_bbox_omits_area(self, handler):
        _, request = handler.transform_params(
            opener_params={"time_range": ["2015-01-01", "2015-01-31"]},
            data_id=f"{_DATASET_NAME}:clara",
        )
        assert "area" not in request

    def test_default_variables_when_omitted(self, handler):
        _, request = handler.transform_params(
            opener_params={"time_range": ["2015-06-01", "2015-06-30"]},
            data_id=f"{_DATASET_NAME}:clara",
        )
        assert set(request["variable"]) == set(_VARIABLES)

    def test_format_is_zip(self, handler):
        _, request = handler.transform_params(
            opener_params={"time_range": ["2015-06-01", "2015-06-30"]},
            data_id=f"{_DATASET_NAME}:clara",
        )
        assert request["format"] == "zip"

    def test_single_month_request(self, handler):
        _, request = handler.transform_params(
            opener_params={"time_range": ["2020-07-01", "2020-07-31"]},
            data_id=f"{_DATASET_NAME}:clara",
        )
        assert request["year"] == ["2020"]
        assert request["month"] == ["07"]

    def test_multi_year_request(self, handler):
        _, request = handler.transform_params(
            opener_params={"time_range": ["2000-11-01", "2001-02-28"]},
            data_id=f"{_DATASET_NAME}:clara",
        )
        assert "2000" in request["year"]
        assert "2001" in request["year"]
        assert "11" in request["month"]
        assert "12" in request["month"]
        assert "01" in request["month"]
        assert "02" in request["month"]

    def test_unknown_data_id_raises(self, handler):
        with pytest.raises(ValueError):
            handler.transform_params(
                opener_params={},
                data_id="unknown-dataset:xyz",
            )


# ---------------------------------------------------------------------------
# _time_range_to_year_months (unit tests for the private helper)
# ---------------------------------------------------------------------------

class TestTimeRangeToYearMonths:
    def test_none_returns_last_year(self):
        import datetime
        years, months = SatelliteSurfaceRadiationBudgetHandler._time_range_to_year_months(None)
        expected_year = str(datetime.date.today().year - 1)
        assert years == [expected_year]
        assert len(months) == 12

    def test_single_month(self):
        years, months = SatelliteSurfaceRadiationBudgetHandler._time_range_to_year_months(
            ["2019-03-01", "2019-03-31"]
        )
        assert years == ["2019"]
        assert months == ["03"]

    def test_cross_year_boundary(self):
        years, months = SatelliteSurfaceRadiationBudgetHandler._time_range_to_year_months(
            ["2010-11-01", "2011-02-28"]
        )
        assert sorted(years) == ["2010", "2011"]
        for m in ["11", "12", "01", "02"]:
            assert m in months