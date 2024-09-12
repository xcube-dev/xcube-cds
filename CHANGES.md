## Changes in 0.9.4 (in development)

 - Move project configuration from `setup.py` to `pyproject.toml`. (#89)
 - Remove `defaults` channel from environment configuration. (#86)
 - Enforce PEP 440 versioning rather than Semver in unit test. (#87)

## Changes in 0.9.3

 - Updates to support new (2024) CDS backend API. (#84)
 - Rename main repository branch from `master` to `main`. (#76)
 - Reformat code using `black`. (#78)
 - Update AppVeyor config for micromamba 2.0. (#82)
 - Update AppVeyor config to use new xcube installation method. (#81)

## Changes in 0.9.2

 - Update dataset handlers to deal with recent changes in the CDS
   backend API (fixes #68).
 - Add `endpoint_url` and `cds_api_key` to store parameters schema
   (fixes #65).
 - Update example notebooks to use the `new_data_store` function
   (closes #70).
 - Provide version number in `xcube_cds.__version__` and `xcube_cds.version`
   rather than `xcube_cds.version.version` (closes #56).
 - Added support for sea ice thickness dataset (closes #61). This includes a
   Jupyter notebook to validate its functionality.

## Changes in 0.9.1

 - Update some dependency versions (most importantly:
   require xcube >= 0.9.0)

## Changes in 0.9.0

 - Replace TypeSpecifier with simpler DataType class
   (addresses https://github.com/dcs4cop/xcube/issues/509)
 - Fix handling of undefined parameters in
   get_open_data_params_schema (commit 0ec84ff4)

## Changes in 0.8.1

 - Use micromamba instead of mamba for CI (closes #43)
 - Use more explicit parameters in xr.open_dataset calls (avoids "cannot
   guess the engine" errors from xarray) (closes #52)

## Changes in 0.8.0

 - Store methods `get_type_specifier` and `get_type_specifiers_for_data` now
   return values in correct format (strings instead of type specifiers)
 - Provided xcube data store framework interface compatibility with
   breaking changes in xcube 0.8.0 (see
   https://github.com/dcs4cop/xcube/issues/420).
 - Soil moisture dataset handler updated to avoid "Request too large" errors
   (closes #48)
 - CdsDataOpener.open_data now logs the xcube-cds version number to stderr.

## Changes in 0.7.0

 - Replace Travis CI with AppVeyor for CI (closes #25)
 - Update demo notebooks to remove dependency on cartopy
 - Document CDS API key usage in store docstring (closes #26)
 - Remove redundant requirements list from setup.py (closes #29)
 - Remove constant-valued parameters from opener schemas (closes #28 and #36)
 - Minor API updates (closes #39)

## Changes in 0.6.0

 - Add release instructions to readme
 - Update open parameters schemas to follow current xcube conventions
 - Exclude ERA5 "monthly averages by hour of day" datasets
 - Add a Travis CI configuration for automated testing
 - Add the ability to specify the CDS API URL and key as parameters to the
   data store and data opener constructors
 - Update and improve the Jupyter demo notebooks
 - Various bug fixes
 - Use the new xcube 0.6 data type specifier system
 - Use new xcube date and date-time schemas
 - Make the "period" and "bbox" parameters optional for the soil moisture
   dataset

## Changes in 0.5.1

 - Add support for the following datasets:
   - ERA5 hourly data on single levels from 1979 to present
   - ERA5-Land hourly data from 1981 to present
   - ERA5-Land monthly averaged data from 1981 to present
   - Soil moisture gridded data from 1978 to present
 - Add dataset investigation scripts to repository
 - Improve code architecture to make it easier to support further datasets 
 - Add a demo notebook for the soil moisture dataset
 - ERA5 datasets: Adjust bounding box to match xcube semantics
 - Several bug fixes

## Changes in 0.5.0

Initial released version.
