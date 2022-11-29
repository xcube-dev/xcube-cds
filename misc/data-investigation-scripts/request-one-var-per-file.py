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
import tarfile


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

    params = request_params['parameters']
    suffix = {'netcdf': 'nc',
              'tgz': 'tgz'}[params['format']]
    for var_name in variable_names:
        # We are only interested in the variable's metadata, so we
        # request a minimal amount of data: one product, one month,
        # one year, one hour, small area.

        dataset_id = request_params['dataset']
        params['variable'] = var_name
        download_dest = args.output_dir + '/' + var_name + '.' + suffix
        c.retrieve(dataset_id, params, download_dest)
        if suffix == 'tgz':
            # For tar files, we assert that they only contain one file,
            # then extract it and give it an 'nc' suffix (under the assumption
            # that it's a NetCDF file).
            with tarfile.open(download_dest) as tf:
                names = tf.getnames()
                assert(len(names) == 1)
                def is_within_directory(directory, target):
                    
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                
                    prefix = os.path.commonprefix([abs_directory, abs_target])
                    
                    return prefix == abs_directory
                
                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                
                    for member in tar.getmembers():
                        member_path = os.path.join(path, member.name)
                        if not is_within_directory(path, member_path):
                            raise Exception("Attempted Path Traversal in Tar File")
                
                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                    
                
                safe_extract(tf, args.output_dir)
                os.rename(args.output_dir + '/' + names[0],
                          args.output_dir + '/' + var_name + '.nc')
                os.remove(download_dest)


if __name__ == '__main__':
    main()
