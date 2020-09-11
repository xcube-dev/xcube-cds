# xcube-cds

## Description

An [xcube plugin](https://xcube.readthedocs.io/en/latest/plugins.html)
which can generate data cubes from the
[Copernicus Climate Data Store (CDS) API](https://cds.climate.copernicus.eu/api-how-to).

Currently supported datasets:

 - [ERA5 hourly data on single levels from 1979 to present](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels)
 - [ERA5-Land hourly data from 1981 to present](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land)
 - [ERA5 monthly averaged data on single levels from 1979 to present](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels-monthly-means?tab=overview)
 - [ERA5-Land monthly averaged data from 1981 to present](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land-monthly-means)
 - [Soil moisture gridded data from 1978 to present](https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-soil-moisture)

## Setup

### Configuring access to the CDS API

In order to use the CDS API via the xcube_cds plugin, you need to obtain a 
CDS user ID (UID) and API key and write them to a configuration file.
Additionally, you need to use the CDS website to agree in advance to the terms
of use for any datasets you want to acccess.

#### Obtain a CDS API key


You can obtain the UID and API key as follows:

1. Create a user account on the
   [CDS Website](https://cds.climate.copernicus.eu/user/register).
2. Log in to the website with your user name and password.
3. Navigate to your [user page](https://cds.climate.copernicus.eu/user/)
   on the website. Your API key is shown at the bottom of the page.

#### Configure CDS API access

Your CDS API key must be made available to the CDS API library. You can do
this by creating a file named `.cdsapirc` in your home directory, with the
following format:

```
url: https://cds.climate.copernicus.eu/api/v2
key: <UID>:<API-KEY>
```

Replace `<UID>` with your UID and `<API-KEY>` with your API key, as obtained
from the CDS website.

#### Agree to the terms of use for the datasets you require

The datasets available through CDS have associated terms of use. Before
accessing a dataset via the API, you must agree to its terms of use, which
can only be done via the CDS website, as follows:

1. [Log in](https://cds.climate.copernicus.eu/user/login) to the CDS website,
   and use the
   [search page](https://cds.climate.copernicus.eu/cdsapp#!/search?type=dataset)
   to find the dataset you require.
2. On the dataset's web page, select the ‘Download data’ tab.
3. Scroll to the bottom of the page, and you will see a section titled
   ‘Terms of use’, which will contain either an ‘Accept terms’ button to
   allow you to accept the terms, or a confirmation that you have already
   accepted the terms.

Once you have accepted the terms on the website, the dataset will also be
made available to you (using the same user credentials) through the API.

### Installing the xcube-cds plugin

#### Installation into a new environment with conda

xcube-cds and all necessary dependencies (including xcube itself) are available
on [conda-forge](https://conda-forge.org/), and can be installed using the
[conda package manager](https://docs.conda.io/projects/conda/en/latest/).
The conda package manager itself can be obtained in the [miniconda
distribution](https://docs.conda.io/en/latest/miniconda.html). Once conda
is installed, xcube-cds can be installed like this:

```
$ conda create --name xcube-cds-environment --channel conda-forge xcube-cds
$ conda activate xcube-cds-environment
```

The name of the environment may be freely chosen.

#### Installation into an existing environment with conda

xcube-cds can also be installed into an existing conda environment.
With the existing environment activated, execute this command:

```
$ conda install --channel conda-forge xcube-cds
```

Once again, xcube and any other necessary dependencies will be installed
automatically if they are not already installed.

#### Installation into an existing environment from the repository

If you want to install xcube-cds directly from the git repository (for example
if order to use an unreleased version or to modify the code), you can
do so as follows:

```
$ conda create --name xcube-cds-environment --channel conda-forge --only-deps xcube-cds
$ git clone https://github.com/dcs4cop/xcube-cds.git
$ cd xcube-cds
$ python setup.py develop
```

## Testing

You can run the unit tests for xcube-cds by executing

```
$ pytest
```

in the `xcube-cds` repository.

To create a test coverage report, you can use

```
coverage run --omit='test/*' --module pytest
coverage html
```

This will write a coverage report to `htmlcov/index.html`.

## Use

Jupyter notebooks demonstrating the use of the xcube-cds plugin can be found
in the `examples/notebooks/` subdirectory of the repository.
