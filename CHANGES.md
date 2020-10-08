## Changes in 0.5.2.dev

 - Add release instructions to readme
 - Update open parameters schemas to follow current xcube conventions
 - Exclude ERA5 "monthly averages by hour of day" datasets
 - Add a Travis CI configuration for automated testing
 - Add the ability to specify the CDS API URL and key as parameters to the
   data store and data opener constructors
 - Update and improve the Jupyter demo notebooks
 - Various bug fixes

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
