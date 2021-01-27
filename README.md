# AzureDataLakeProcessingScripts

Code to access and download the EC tower data from the Azure data lake. The access protocols are left out of this repo for security reasons.

## Setup

Many of the included scripts require input parameters to run. Because some of these parameters are sensitive they are not included in this repository. To run these scripts you must first:

1. Create a `.secret` directory at the root level
2. Copy `DataLakeDownload_TEMPLATE.xlsx` into the `.secret` folder and rename to `DataLakeDownload.xlsx`
3. Fill in the necessary information in the copied xlsx file

## Documents

### DataLakeDownload_TEMPLATE.xlsx

- Contains the options, access values, and input/output filepaths for both a local machine and the Azure Datalake
- Is an Excel sheet with 5 workbooks, one for each site plus and explainer for the different rows and columns within each.
- Is required to run the scripts in, most of the options are contained here along with some QC components. A few minor considerations are still hard-coded within the driver script but will be discussed below.
- THe row and column headers are matched exactly within the code and messing with these will cause the code to error as a "Key Error" for a mismatched index or column.

## Features still to add

- Meteorology plot report generation akin to the flux tower report generation
- Add further QC options to flux data (despike; localized QC for sensor removals) if warranted

## Acknowledgements

Original code was written by Dr. Eric Russell ([esrussell16](https://github.com/esrussell16)), eric.s.russell@wsu.edu

## License

As a work of the United States government, this project is in the public domain within the United States.

Additionally, we waive copyright and related rights in the work worldwide through the CC0 1.0 Universal public domain dedication.