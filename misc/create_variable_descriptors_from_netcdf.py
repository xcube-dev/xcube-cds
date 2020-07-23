#!/usr/bin/env python3

"""Create variable descriptor source code from a NetCDF file.

An xcube data store plugin must implement a describe_data method returning
a DataDescriptor (which in turn contains VariableDescriptors) for each
supported data ID. This script takes a NetCDF file as its first and only
argument and prints Python source code to create corresponding
VariableDescriptors.
"""

import sys
from netCDF4 import Dataset


def main():
    with Dataset(sys.argv[1], 'r') as ds:
        for vname, vinfo in ds.variables.items():
            if '_CoordinateAxisType' not in vinfo.ncattrs():
                output_variable(vname, vinfo)


class VariableDescriptor:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def constructor_string(self):
        output = []
        for arg, value in self.kwargs.items():
            output.append(f"{arg}={self._fmt(value)}")
        return 'VariableDescriptor(\n    ' + ',\n    '.join(output) + '\n),'

    @staticmethod
    def _fmt(value):
        return repr(value) if type(value) in (tuple, dict, list) \
            else f"'{value}'"


def output_variable(vname, vinfo):
    attr_map = {}
    for attr_name in 'units', 'long_name':
        if attr_name in vinfo.ncattrs():
            attr_map[attr_name] = vinfo.getncattr(attr_name)

    vd = VariableDescriptor(name=vname, dtype=vinfo.datatype,
                            dims=vinfo.dimensions,
                            attrs=attr_map)
    print(vd.constructor_string())


if __name__ == '__main__':
    main()
