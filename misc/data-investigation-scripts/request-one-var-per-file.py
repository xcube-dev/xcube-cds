#!/usr/bin/env python3

"""Using cdsapi, fetch a set of single-variable NetCDF files.

This script helps to determine the relationship between CDS API request
variables and their corresponding output variables in the ensuing NetCDF
file. It does this by making a CDS API request for each of a predefined
list of request variables, and saving each of the resulting NetCDF files
with a name corresponding to the CDS API variable used to request it.
The list of request variables can be extracted from the CDS web interface.
"""


import cdsapi
import os
import argparse
import json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-dir', default='nc-single-var')
    parser.add_argument('variable_list')
    parser.add_argument('request')
    args = parser.parse_args()

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    print(args)
    with open(args.variable_list, 'r') as fh:
        variable_names = [line.strip() for line in fh.readlines()]

    with open(args.request) as fh:
        request_params = json.load(fh)

    c = cdsapi.Client()
    
    for var_name in variable_names:
        # We are only interested in the variable's metadata, so we
        # request a minimal amount of data: one product, one month,
        # one year, one hour, small area.

        dataset_id = request_params['dataset']
        params = request_params['parameters']
        params['variable'] = var_name

        c.retrieve(
            dataset_id,
            params,
            args.output_dir + '/' + var_name + '.nc')


if __name__ == '__main__':
    main()
