# The MIT License (MIT)
# Copyright (c) 2026 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Handler for the CDS dataset
'satellite-surface-radiation-budget'
(Surface radiation budget from 1979 to present derived from satellite
observations).

Two product families are available:

* **CLARA** – CM SAF cLoud, Albedo and surface Radiation dataset (AVHRR-based,
  produced by EUMETSAT CM SAF).  Monthly means on a 0.25 ° × 0.25 ° grid.
* **CCI** – Cloud Climate Change Initiative products (ESA/C3S, ATSR2/AATSR and
  SLSTR-based, produced by STFC RAL Space).  Monthly means on a 0.5 ° × 0.5 °
  grid.

Both families cover the same seven surface radiation variables:

=================================  ===  =======================================
CDS download name                  Abbr Description
=================================  ===  =======================================
surface_incoming_shortwave_radiation SIS  Incoming (downwelling) solar flux
surface_outgoing_shortwave_radiation SRS  Reflected (upwelling) solar flux
surface_net_shortwave_radiation    SNS  Net solar flux  (SIS − SRS)
surface_downwelling_longwave_radiation SDL  Downwelling thermal flux
surface_outgoing_longwave_radiation SOL  Upwelling thermal flux
surface_net_longwave_radiation     SNL  Net longwave flux (SDL − SOL)
surface_radiation_budget           SRB  Total net flux (SNS + SNL)
=================================  ===  =======================================

Data IDs exposed by this handler
---------------------------------
``satellite-surface-radiation-budget:clara``
``satellite-surface-radiation-budget:cci:esa``
``satellite-surface-radiation-budget:cci:c3s``

References
----------
https://cds.climate.copernicus.eu/datasets/satellite-surface-radiation-budget
DOI: 10.24381/cds.cea58b5a
"""

from typing import Any, Iterator, Tuple

import xarray as xr

from xcube_cds.store import CDSDatasetHandler

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DATASET_NAME = "satellite-surface-radiation-budget"

_VARIABLES = [
    "surface_incoming_shortwave_radiation",
    "surface_outgoing_shortwave_radiation",
    "surface_net_shortwave_radiation",
    "surface_downwelling_longwave_radiation",
    "surface_outgoing_longwave_radiation",
    "surface_net_longwave_radiation",
    "surface_radiation_budget",
]

# Mapping: data_id suffix  →  (product_family, origin)
_DATA_ID_MAP = {
    "clara": ("clara_a3", "eumetsat"),
    "cci:esa": ("cci", "esa"),
    "cci:c3s": ("cci", "c3s"),
}

# Years covered by each sub-dataset (approximate; check CDS for exact bounds)
_YEAR_RANGES = {
    "clara": (1979, 2023),
    "cci:esa": (1982, 2016),
    "cci:c3s": (2017, 2023),
}

_MONTHS = [f"{m:02d}" for m in range(1, 13)]


# ---------------------------------------------------------------------------
# Handler implementation
# ---------------------------------------------------------------------------

class SatelliteSurfaceRadiationBudgetHandler(CDSDatasetHandler):
    """xcube-cds dataset handler for *satellite-surface-radiation-budget*.

    Supports both the CLARA and CCI product families available through the
    Copernicus Climate Data Store.
    """

    # ------------------------------------------------------------------
    # CDSDatasetHandler interface
    # ------------------------------------------------------------------

    def get_data_ids(self) -> Iterator[Tuple[str, None]]:
        for suffix in _DATA_ID_MAP:
            yield f"{_DATASET_NAME}:{suffix}", None

    def get_open_data_params_schema(self, data_id: str) -> dict:
        """Return a JSON-Schema describing the open-data parameters."""
        suffix = self._get_suffix(data_id)
        year_min, year_max = _YEAR_RANGES[suffix]

        return {
            "type": "object",
            "properties": {
                "variable_names": {
                    "type": "array",
                    "items": {"type": "string", "enum": _VARIABLES},
                    "default": _VARIABLES,
                    "title": "Variables",
                    "description": (
                        "List of surface radiation budget variables to retrieve."
                    ),
                },
                "time_range": {
                    "type": "array",
                    "items": {"type": "string", "format": "date"},
                    "minItems": 2,
                    "maxItems": 2,
                    "title": "Time range",
                    "description": (
                        "Start and end date (inclusive) in ISO 8601 format "
                        "(YYYY-MM-DD).  Only full months are retrieved."
                    ),
                },
                "bbox": {
                    "type": "array",
                    "items": {"type": "number"},
                    "minItems": 4,
                    "maxItems": 4,
                    "title": "Bounding box",
                    "description": (
                        "[west, south, east, north] in decimal degrees "
                        "(WGS 84).  If omitted the global extent is returned."
                    ),
                },
            },
            "additionalProperties": False,
        }

    def get_data_source_info(self, data_id: str) -> dict:
        suffix = self._get_suffix(data_id)
        product_family, origin = _DATA_ID_MAP[suffix]
        year_min, year_max = _YEAR_RANGES[suffix]
        return {
            "cds_dataset_name": _DATASET_NAME,
            "product_family": product_family,
            "origin": origin,
            "temporal_coverage_start": f"{year_min}-01-01",
            "temporal_coverage_end": f"{year_max}-12-31",
            "time_aggregation": "monthly_mean",
            "data_type": "gridded",
        }

    def transform_params(
        self,
        opener_params: dict,
        data_id: str,
    ) -> Tuple[str, dict]:
        """Translate xcube open-data parameters into a CDS API request dict."""
        suffix = self._get_suffix(data_id)
        product_family, origin = _DATA_ID_MAP[suffix]

        variable_names = opener_params.get("variable_names", _VARIABLES)
        time_range = opener_params.get("time_range")
        bbox = opener_params.get("bbox")

        # Build year / month lists from time_range
        years, months = self._time_range_to_year_months(time_range)

        request: dict[str, Any] = {
            "product_family": product_family,
            "origin": origin,
            "variable": variable_names,
            "time_aggregation": "monthly_mean",
            "climate_data_record_type": "thematic_climate_data_record",
            "year": years,
            "month": months,
            "format": "zip",
        }

        if bbox is not None:
            west, south, east, north = bbox
            request["area"] = [north, west, south, east]

        return _DATASET_NAME, request

    def load_from_netcdf(
        self,
        paths: list,
        opener_params: dict,
        data_id: str,
    ) -> xr.Dataset:
        """Open and merge all NetCDF files downloaded for the request."""
        ds = xr.open_mfdataset(
            paths,
            combine="by_coords",
            decode_times=True,
        )
        # Rename spatial coords to xcube convention if needed
        rename_map = {}
        if "lat" in ds.dims and "latitude" not in ds.dims:
            rename_map["lat"] = "latitude"
        if "lon" in ds.dims and "longitude" not in ds.dims:
            rename_map["lon"] = "longitude"
        if rename_map:
            ds = ds.rename(rename_map)
        return ds

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------

    @staticmethod
    def _get_suffix(data_id: str) -> str:
        """Extract and validate the sub-dataset suffix from a full data_id."""
        prefix = f"{_DATASET_NAME}:"
        if not data_id.startswith(prefix):
            raise ValueError(
                f"Unknown data_id '{data_id}'.  Expected one of: "
                + ", ".join(f"{_DATASET_NAME}:{s}" for s in _DATA_ID_MAP)
            )
        suffix = data_id[len(prefix):]
        if suffix not in _DATA_ID_MAP:
            raise ValueError(
                f"Unknown sub-dataset '{suffix}'.  Expected one of: "
                + ", ".join(_DATA_ID_MAP.keys())
            )
        return suffix

    @staticmethod
    def _time_range_to_year_months(
        time_range: list | None,
    ) -> Tuple[list[str], list[str]]:
        """Convert an optional [start, end] date pair to sorted year/month lists.

        If *time_range* is None all months of all years in the handler's full
        catalogue range cannot be determined here (the CDS API will accept
        partial lists), so a reasonable default of the last complete year is
        returned as a fallback.
        """
        import datetime

        if time_range is None:
            # Sensible default: most recent complete year
            current_year = datetime.date.today().year - 1
            return [str(current_year)], _MONTHS

        start = _parse_date(time_range[0])
        end = _parse_date(time_range[1])

        years: set[str] = set()
        months: set[str] = set()

        current = start.replace(day=1)
        while current <= end:
            years.add(str(current.year))
            months.add(f"{current.month:02d}")
            # advance by one month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        return sorted(years), sorted(months)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _parse_date(date_str: str):
    """Parse a date string of the form YYYY-MM-DD (or YYYY-MM or YYYY)."""
    import datetime

    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: '{date_str}'")