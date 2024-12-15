# kedro-tdda

This plugin extends Kedro with a lightweight data validation tool: `tdda`.
The `tdda` package enables you to discover constraints from data, 
validate new data and detect anomalies. 
Check http://tdda.readthedocs.io for more info about `tdda`. 

This plugin is limited to it's constraints API and alike tdda only supports pandas datasets. 


## Installation

You can install the package like so

```
pip install git+https://github.com/sinanpl/kedro_tdda
```

## Usage

The plugin currently uses the constraints API of tdda. The available commands are 
- `discover`: infer constraints for a dataset and write to yaml. Constraints are inferred by field. Examples are:
    - `type`: one of int, real, bool, string or date
    - `min`: minimum allowed value,
    - `max`: maximum allowed value,
    - `min_length`: minimum allowed string length (for string fields),
    - `max_length`: maximum allowed string length (for string fields),
    - `max_nulls`: maximum number of null values allowed,
    - `sign`: one of positive, negative, non-positive, non-negative,
    - `no_duplicates`: true if the field values must be unique,
    - `values`: list of distinct allowed values,
    - `rex`: list of regular expressions, to cover all cases
- `verify`: verify if the dataset - after being updated - respects the constraints.
- `detect`: write a csv records file with deviating rows

```sh
kedro tdda discover -h
```
```
Options:
  -d, --dataset TEXT  The name of the pandas catalog entry for which
                      constraints be inferred.
  -e, --env TEXT      The kedro environment where the dataset to retrieve is
                      available. Default to 'base'
  -o, --overwrite     Boolean indicator for overwriting an existing tdda
                      constrains yml specification
  -h, --help          Show this message and exit.```
```
