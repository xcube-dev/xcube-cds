#!/usr/bin/env python3

"""Print variable metadata and filenames of single-variable NetCDF files.

This script is designed to be run on the single-variable NetCDF files
produced by the request-one-var-per-file.py script. For each file, it
prints the filename, variable name, and values of the 'units' and
'long_name' attributes to the standard output.
"""

import xarray as xr
import os
import re
import json
import sys
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-dir', default='nc-single-var')
    args = parser.parse_args()

    param_table = []
    filenames = os.listdir(args.input_dir)
    for filename in filenames:
        param_table.append(read_param_data(args.input_dir, filename))
    param_table.sort()
    json.dump(param_table, sys.stdout, indent=2)

        
def read_param_data(parent_dir, filename):
    param_name_request = re.sub('[.]nc$', '', filename)
    with xr.open_dataset(os.path.join(parent_dir, filename),
                         decode_cf=True) as dataset:
        var_names_all = set(dataset.variables.keys())
        var_names = var_names_all - {'latitude', 'longitude', 'time'}
        if len(var_names) != 1:
            raise Exception(f'File must contain exactly one data variable, '
                            f'not {len(var_names)}.')
        (var_name, ) = var_names
        attrs = dataset.variables[var_name].attrs
    return (param_name_request, var_name, attrs['units'],
            attrs['long_name'])


if __name__ == '__main__':
    main()
