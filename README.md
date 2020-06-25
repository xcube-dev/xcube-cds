# xcube-cds

An [xcube plugin](https://xcube.readthedocs.io/en/latest/plugins.html)
which can generate data cubes from the
[Copernicus Climate Data Store (CDS) API](https://cds.climate.copernicus.eu/api-how-to).

Currently supported datasets:

 - [ERA5 monthly averaged data on single levels from 1979 to present](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels-monthly-means?tab=overview).

## Setup

xcube and cdsapi must be installed before the xcube_cds plugin.
In order to use the xcube_cds plugin, you also need to obtain a CDS API key
and write it to a configuration file. Additionally, you need to use the CDS
website to agree to the terms of use for any datasets you acccess. These
steps are described in more detail below.

### Install xcube

[xcube](https://github.com/dcs4cop/xcube) can be installed from
[conda-forge](https://conda-forge.org/) using the
conda package manager, like this:

```
$ conda create --name xcube xcube>=0.4
$ conda activate xcube
```

xcube can also be built from source, like this:

```
$ git clone https://github.com/dcs4cop/xcube.git
$ cd xcube
$ conda env create
$ conda activate xcube
$ python setup.py develop
```

### Install cdsapi

xcube_cds makes use of the
[cdsapi Python library](https://github.com/ecmwf/cdsapi) to connect to the CDS
API. You can install it into an xcube environment using pip:

```
$ conda activate xcube
$ pip install cdsapi
```

### Install xcube_cds

Currently, xcube_cds is not available via conda, so must be built from source
after activating an xcube conda environment:

```
$ conda activate xcube
$ git clone https://github.com/dcs4cop/xcube-cds.git
$ cd xcube-cds
$ python setup.py develop
```

Once xcube_cds becomes available on conda-forge, it will be possible to install
it like this:

```
$ conda activate xcube
$ conda install -c conda-forge xcube_cds
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
