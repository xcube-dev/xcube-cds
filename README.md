[![Build Status](https://ci.appveyor.com/api/projects/status/urs0anenn7rujy1v/branch/main?svg=true)](https://ci.appveyor.com/project/bcdev/xcube-cds)

# xcube-cds

An [xcube plugin](https://xcube.readthedocs.io/en/latest/plugins.html)
which can generate data cubes from the
[Copernicus Climate Data Store (CDS) API](https://cds-beta.climate.copernicus.eu/how-to-api).

## Description

xcube-cds uses the ECMWF's [cdsapi](https://github.com/ecmwf/cdsapi) library,
along with user-supplied CDS account details, to fetch data from the Copernicus
CDS.

Currently supported datasets:

 - [ERA5 hourly data on single levels from 1940 to present](https://cds-beta.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=overview)
 - [ERA5-Land hourly data from 1950 to present](https://cds-beta.climate.copernicus.eu/datasets/reanalysis-era5-land?tab=overview)
 - [ERA5 monthly averaged data on single levels from 1940 to present](https://cds-beta.climate.copernicus.eu/datasets/reanalysis-era5-single-levels-monthly-means?tab=overview)
 - [ERA5-Land monthly averaged data from 1950 to present](https://cds-beta.climate.copernicus.eu/datasets/reanalysis-era5-land-monthly-means?tab=overview)
 - [Soil moisture gridded data from 1978 to present](https://cds-beta.climate.copernicus.eu/datasets/satellite-soil-moisture?tab=overview)
 - [Sea ice thickness gridded data from 2002 to present](https://cds-beta.climate.copernicus.eu/datasets/satellite-sea-ice-thickness?tab=overview)

## Setup

### Configuring access to the CDS API

In order to use the CDS API via the xcube_cds plugin, you need to obtain a 
Personal Access Token and write it to a configuration file.
Additionally, you need to use the CDS website to agree in advance to the terms
of use for any datasets you want to acccess.

#### Obtain a CDS Personal Access Token

You can obtain a CDS Personal Access Token as follows:

1. Create a user account on the
   [CDS Website](https://cds-beta.climate.copernicus.eu/).
2. Log in to the website with your username and password.
3. Navigate to your [user page](https://cds-beta.climate.copernicus.eu/profile), 
   where you can find your Personal Access Token.

#### Configure CDS API access

Your CDS Personal Access Token must be made available to the CDS API library.
You can do this by creating a file named `.cdsapirc` in your home directory,
with the following format:

```
url: https://cds-beta.climate.copernicus.eu/api
key: <PERSONAL-ACCESS-TOKEN>
```

Replace `<PERSONAL-ACCESS-TOKEN>` with your Personal Access Token.

#### Agree to the terms of use for the datasets you require

The datasets available through CDS have associated terms of use. Before
accessing a dataset via the API, you must agree to its terms of use, which
can only be done via the CDS website, as follows:

1. [Log in](https://cds-beta.climate.copernicus.eu) to the CDS website,
   and go to 'Datasets' to find the dataset you require.
2. On the dataset's web page, select the ‘Download’ tab.
3. Scroll to the bottom of the page, and you will see a section titled
   ‘Terms of use’, which will contain either an ‘Accept terms’ button to
   allow you to accept the terms, or a confirmation that you have already
   accepted the terms.

Once you have accepted the terms on the website, the dataset will also be
made available to you through the API.

### Installing the xcube-cds plugin

This section describes three alternative methods you can use to install the
xcube-cds plugin.

For installation of conda packages, we recommend
[mamba](https://mamba.readthedocs.io/). It is also possible to use conda,
but note that installation may be significantly slower with conda than with
mamba. If using conda rather than mamba, replace the `mamba` command with
`conda` in the installation commands given below.

#### Installation into a new environment with mamba

This method creates a new conda environment and installs the latest conda-forge
release of xcube-cds, along with all its required dependencies, into the
newly created environment.

xcube-cds and all necessary dependencies (including xcube itself) are available
on [conda-forge](https://conda-forge.org/), and can be installed using the
[conda package manager](https://docs.conda.io/projects/conda/en/latest/).
The conda package manager itself can be obtained in the [miniconda
distribution](https://docs.conda.io/en/latest/miniconda.html). Once conda
is installed, xcube-cds can be installed like this:

```bash
mamba create --name xcube-cds-environment --channel conda-forge xcube-cds
mamba activate xcube-cds-environment
```

The name of the environment may be freely chosen.

#### Installation into an existing environment with mamba

This method assumes that you have an existing conda environment and you want
to install xcube-cds into it.

xcube-cds can also be installed into an existing conda environment.
With the existing environment activated, execute this command:

```bash
mamba install --channel conda-forge xcube-cds
```

Once again, xcube and any other necessary dependencies will be installed
automatically if they are not already installed.

#### Installation into an existing environment from the repository

If you want to install xcube-cds directly from the git repository (for example
if order to use an unreleased version or to modify the code), you can
do so as follows:

```bash
mamba create --name xcube-cds-environment --channel conda-forge --only-deps xcube-cds
mamba activate xcube-cds-environment
git clone https://github.com/dcs4cop/xcube-cds.git
python -m pip install --no-deps --editable xcube-cds/
```

This installs all the dependencies of xcube-cds into a fresh conda environment,
then installs xcube-cds into this environment from the repository.

## Testing

You can run the unit tests for xcube-cds by executing

```bash
pytest
```

in the `xcube-cds` repository. Note that, in order to successfully run the
tests using the current repository version of xcube-cds, you may also need to
install the repository version of xcube rather than its latest conda-forge
release.

To create a test coverage report, you can use

```bash
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

-   Make sure that dependencies and their versions are up to date in
    the `environment.yml` file. In particular, check that the required xcube 
    version is correct and that required version numbers of packages which are 
    also transitive xcube dependencies (e.g. xarray) are compatible with
    xcube's requirements.
-   Make sure that all unit tests pass and that test coverage is 100% (or
    as near to 100% as practicable).
-   Remove any pre-release (‘dev’, ‘rc’ etc.) suffix from the version number in
    xcube_cds/version.py`.
-   Make sure that the readme and changelog are up to date. Remove any
    pre-release suffix from the current (first) section title of the changelog.

### Making a GitHub release

Create a release tag on GitHub.

-   Tag version name should be the version number prefixed by ‘v’.
-   Release title should be version name without a prefix.
-   Description should be a list of changes in this version (pasted in
    from most recent section of changelog).
   
Creating the release will automatically produce a source code archive as an
associated asset, which will be needed to create the conda package.

### Updating the conda package

These instructions are based on the documentation at
https://conda-forge.org/docs/maintainer/updating_pkgs.html .

Conda-forge packages are produced from a github feedstock repository belonging
to the conda-forge organization. The feedstock for xcube-cds is at
https://github.com/conda-forge/xcube-cds-feedstock . The package is updated
by forking this repository (to a personal GitHub account, not to an
organization), creating a new branch for the changes, and
creating a pull request to merge this branch into conda-forge's feedstock
repository.

The instructions below describe the manual procedure of creating a branch and
pull request yourself. conda-forge also has a bot called
`regro-cf-autotick-bot` which should automatically create a branch and pull
request for each new GitHub release, within about an hour or two of the
release appearing on GitHub. The bot also automatically updates the version
number and release hash in the recipe, but further manual modifications (in
the form of commits pushed to the PR) may be required, for example in order to
update dependency lists. The manual procedure may be necessary or desirable in
some cases, for example:

-   You are making an additional build from a version which has already been
    released in conda-forge. In this case there's no new GitHub release to
    trigger the bot.
  
-   You don't have time to wait for the bot to notice the new release and
    create its pull request.

If you are basing the release on the automatically created pull request, skip
the "create a new branch" and "create a pull request" steps below, and instead
make the necessary changes (if any) on the branch created by the bot. If a
release is made using a manually created branch, the bot will remove its own
branch and pull request. (Sometimes it's necessary to apply the `bot-rerun`
label to the pull request to make this happen.)

In detail, the steps are as follows.

1.  Fork the feedstock repository to a personal GitHub account (not an
    organization).

2.  Clone the repository locally and create a new branch. The name of the
    branch is not strictly prescribed, but it's sensible to choose an
    informative name like `update_0_5_3`.

3.  Rerender the feedstock using `conda-smithy`. This updates common
    conda-forge feedstock files. It's probably easiest to install
    `conda-smithy` in a fresh environment for this.
   
    ```bash
    mamba create -c conda-forge -n smithy conda-smithy
    mamba activate smithy
    conda-smithy rerender -c auto
    ```
   
    It's also possible to have the rendering done by a bot as part of the pull
    request, but this may fail if it doesn't have the necessary permissions.

4.  Update `recipe/meta.yaml` for the new version. Mainly this will involve the 
    following steps:
    
    1.  Update the value of the `version` variable (or, if the version number
        has not changed, increment the build number).
     
    2.  If the version number *has* changed, ensure that the build number is
        set to 0.
     
    3.  Update the sha256 hash of the source archive prepared by GitHub.
     
    4.  If the dependencies have changed, update the list of dependencies in
        the `-run` subsection to match those in the `environment.yml` file.

    5.  Make sure that the list of recipe maintainers (in the
        `extra.recipe-maintainers` section of the `meta.yaml` file) is up
        to date.

    6.  Make sure that the list of code owners (in `.github/CODEOWNERS`)
        is up to date.

5.  Commit the changes and push them to GitHub, then create a pull request
    from your branch for a merge of your update branch into the main branch
    at conda-forge.
 
6.  Once conda-forge's automated checks have passed and the reviewers (if
    any) have approved the changes, merge the pull request.

Once the pull request has been merged, the updated package should become
available from conda-forge within a couple of hours. Usually the updated
package is visible on https://anaconda.org/conda-forge/xcube-cds some time
before it becomes accessible to `mamba search` and `mamba install`.

### Post-release tasks

 - Update the version number in `xcube_cds/version.py` to a "dev0" derivative 
   of the next planned release number. For example, if version 0.5.1 has just 
   been released and the next version is planned to be 0.5.2, the version 
   number should be set to 0.5.2.dev0.

 - Add a new first section to the changelog with the new version number.

