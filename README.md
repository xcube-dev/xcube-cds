# xcube-cds

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

[xcube](https://github.com/dcs4cop/xcube) and cdsapi must be installed
before the xcube_cds plugin.
In order to use the xcube_cds plugin, you also need to obtain a CDS API key
and write it to a configuration file. Additionally, you need to use the CDS
website to agree to the terms of use for any datasets you acccess. These
steps are described in more detail below.

### Install xcube

xcube is available on [conda-forge](https://conda-forge.org/),
and can be installed using the
[conda package manager](https://docs.conda.io/projects/conda/en/latest/),
like this:

```
$ conda create --name xcube xcube>=0.5
$ conda activate xcube
```

Alternatively, the latest version of xcube can be installed from the soure code
repository like this:

```
$ git clone https://github.com/dcs4cop/xcube.git
$ cd xcube
$ conda env create
$ conda activate xcube
$ python setup.py develop
```

### Install cdsapi

xcube_cds makes use of the
[cdsapi Python library](https://github.com/ecmwf/cdsapi),
which is available from conda-forge, to connect to the CDS API.
You can install it into an xcube environment like this:

```
$ conda activate xcube
$ conda install -c conda-forge cdsapi
```

### Install xcube_cds

xcube_cds is also available on conda-forge, and can be installed like this:

```
$ conda activate xcube
$ conda install -c conda-forge xcube-cds
```

Alternatively, the latest version of xcube_cds can be installed from the
source code repository like this:

```
$ conda activate xcube
$ git clone https://github.com/dcs4cop/xcube-cds.git
$ cd xcube-cds
$ conda env update
$ python setup.py develop
```

### Obtain an API key

In order to access the CDS API via the xcube_cds plugin, you need a CDS user
ID (UID) and API key. You can obtain these as follows:

1. Create a user account on the
   [CDS Website](https://cds.climate.copernicus.eu/user/register).
2. Log in to the website with your user name and password.
3. Navigate to your [user page](https://cds.climate.copernicus.eu/user/)
   on the website. Your API key is shown at the bottom of the page.

### Configure CDS API access

Your CDS API key must be made available to the CDS API library. You can do
this by creating a file named `.cdsapirc` in your home directory, with the
following format:

```
url: https://cds.climate.copernicus.eu/api/v2
key: <UID>:<API-KEY>
```

Replace `<UID>` with your UID and `<API-KEY>` with your API key, as obtained
from the CDS website.

### Agree to the terms of use for the datasets you require

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

### Test xcube_cds

You can run the unit tests for xcube_cds by executing

```
$ pytest
```

in the `xcube-cds` repository.
