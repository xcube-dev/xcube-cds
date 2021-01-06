[![Build Status](https://ci.appveyor.com/api/projects/status/urs0anenn7rujy1v/branch/master?svg=true)](https://ci.appveyor.com/project/bcdev/xcube-cds)

# xcube-cds

An [xcube plugin](https://xcube.readthedocs.io/en/latest/plugins.html)
which can generate data cubes from the
[Copernicus Climate Data Store (CDS) API](https://cds.climate.copernicus.eu/api-how-to).

## Description

xcube-cds uses the ECMWF's [cdsapi](https://github.com/ecmwf/cdsapi) library,
along with user-supplied CDS account details, to fetch data from the Copernicus
CDS.

Currently supported datasets:

 - [ERA5 hourly data on single levels from 1979 to present](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels)
 - [ERA5-Land hourly data from 1981 to present](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land)
 - [ERA5 monthly averaged data on single levels from 1979 to present](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-single-levels-monthly-means?tab=overview)
 - [ERA5-Land monthly averaged data from 1981 to present](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land-monthly-means)
 - [Soil moisture gridded data from 1978 to present](https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-soil-moisture)
   (Note that geographic subsetting for this dataset is implemented within the
   CDS Store, so even for small bounding boxes the full global data must be
   fetched from the CDS API.)

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

You can specify an alternative location for the CDS API configuration file
using the `CDSAPI_RC` environment variable, or provide the URL and key
without a configuration file by setting the `CDSAPI_URL` and `CDSAPI_KEY`
environment variables. You can also pass the URL and key directly to the
`CDSDataOpener` and `CDSDataStore` constructors using the named parameters
`cds_api_url` and `cds_api_key`.
 
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

in the `xcube-cds` repository. Note that, in order to successfully run the
tests using the current repository version of xcube-cds, you may also need to
install the repository version of xcube rather than its latest conda-forge
release.

To create a test coverage report, you can use

```
coverage run --include='xcube_cds/**' --module pytest
coverage html
```

This will write a coverage report to `htmlcov/index.html`.

## Use

Jupyter notebooks demonstrating the use of the xcube-cds plugin can be found
in the `examples/notebooks/` subdirectory of the repository.

## Preparing a release

This section is intended for developers preparing a new release of xcube-cds.

### Pre-release tasks

 - Make sure that all unit tests pass and that test coverage is 100% (or
   as near to 100% as practicable).
 - Remove any pre-release (‘dev’, ‘rc’ etc.) suffix from the version number in
   `xcube_cds/version.py`.
 - Make sure that the readme and changelog are up to date. Remove any
   pre-release suffix from the current (first) section title of the changelog.

### Making a GitHub release

Create a release tag on GitHub.

 - Tag version name should be the version number prefixed by ‘v’.
 - Release title should be version name without a prefix.
 - Description should be a list of changes in this version (pasted in
   from most recent section of changelog).
   
Creating the release will automatically produce a source code archive as an
associated asset, which will be needed to create the conda package.

### Updating the conda package

These instructions are based on the documentation at
https://conda-forge.org/docs/maintainer/updating_pkgs.html .

Conda-forge packages are produced from a github feedstock repository belonging
to the conda-forge organization. The feedstock for xcube-cds is at
https://github.com/conda-forge/xcube-cds-feedstock . The package is updated
by forking this repository, creating a new branch for the changes, and
creating a pull request to merge this branch into conda-forge's feedstock
repository. dcs4cop's fork is at https://github.com/dcs4cop/xcube-cds-feedstock
.

The instructions below describe the manual procedure of creating branch and
pull request yourself. conda-forge also has a bot called
`regro-cf-autokick-bot` which should automatically create a pull request for
each new GitHub release, within about an hour or two of the release appearing
on GitHub. If you prefer to base the release on the automatically created
pull request, skip the "create a new branch" and "create a pull request"
steps below, and instead make the necessary changes (if any) on the branch
created by the bot. If a release is made using a manually created branch,
the bot will remove its own branch and pull request. (Sometimes it's necessary
to apply the `bot-rerun` label to the pull request to make this happen.)

In detail, the steps are as follows.

1. Update the [dcs4cop fork](https://github.com/dcs4cop/xcube-cds-feedstock)
   of the feedstock repository, if it's not already up to date with
   conda-forge's upstream repository.

2. Clone the repository locally and create a new branch. The name of the branch
   is not strictly prescribed, but it's sensible to choose an informative
   name like `update_0_5_3`.

3. Rerender the feedstock using `conda-smithy`. This updates common conda-forge
   feedstock files. It's probably easiest to install `conda-smithy` in a fresh
   environment for this.
   
   ```
   conda install -c conda-forge conda-smithy
   conda smithy rerender -c auto
   ```
   
   It's also possible to have the rendering done by a bot as part of the pull
   request, but this may fail if it doesn't have the necessary permissions.

4. Update `recipe/meta.yaml` for the new version. Mainly this will involve the 
   following steps:
   
   1. Update the value of the `version` variable (or, if the version number
      has not changed, increment the build number).
   
   2. If the version number *has* changed, ensure that the build number is
      set to 0.
   
   3. Update the sha256 hash of the source archive prepared by GitHub.
   
   4. If the dependencies have changed, update the list of dependencies in the
      `-run` subsection to match those in the `environment.yml` file.

   5. Commit the changes and push them to GitHub, then create a pull request at
      https://github.com/dcs4cop/xcube-cds-feedstock .

   6. Once conda-forge's automated checks have passed, merge the pull request.

Once the pull request has been merged, the updated package should usually 
become available from conda-forge within a couple of hours.


### Post-release tasks

 - Update the version number in `xcube_cds/version.py` to a "dev0" derivative 
   of the next planned release number. For example, if version 0.5.1 has just 
   been released and the next version is planned to be 0.5.2, the version 
   number should be set to 0.5.2.dev0.

 - Add a new first section to the changelog with the new version number.

 - Merge the changes from https://github.com/conda-forge/xcube-cds-feedstock
   back to the master branch of https://github.com/dcs4cop/xcube-cds-feedstock,
   so that it's ready to serve as a base for the next update branch.
