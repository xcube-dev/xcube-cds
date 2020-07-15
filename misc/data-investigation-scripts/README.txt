This directory contains two scripts which can be used to document the
output data formats produced fromthe CDS API. They were written for the
ERA5 dataset, but may be useful for others.

request-one-var-per-file.py takes a hard-coded list of request variable
names (obtained from the CDS web interface) and, for each of them, obtains
from the CDS API a NetCDF file containing data for only that variable.
The file is saved with the name of the request variable used to obtain it.

read-var-metadata-from-nc.py reads the files produced by
request-one-var-per-file.py and prints out a JSON structure giving, for each
file, the filename, the variable name, and some variable attributes. In effect
it produces a table showing the relationship between request variable, the
corresponding (but often very differently named) NetCDF variable, and other
variable metadata.
