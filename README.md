# kedro-tdda

This plugin extends `kedro` with a lightweight data validation tool: `tdda`.
The `tdda` package enables you to **discover** constraints from data, 
**validate** new data and **detect anomalies**. 
Check the [tdda website](http://tdda.readthedocs.io) for more info. 

This `kedro` plugin is limited to it's **constraints API** and alike `tdda` only supports `pandas` datasets.
The plugin includes a CLI interface and dataset hooks

For more extensive needs, please check [`kedro_pandera`](https://github.com/Galileo-Galilei/kedro-pandera)

## Installation

You can install the package like so

```bash
pip install git+https://github.com/sinanpl/kedro_tdda
```

## Usage

### CLI

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
```sh
Options:
  -d, --dataset TEXT  The name of the pandas catalog entry for which
                      constraints be inferred.
  -e, --env TEXT      The kedro environment where the dataset to retrieve is
                      available. Default to 'base'
  -o, --overwrite     Boolean indicator for overwriting an existing tdda
                      constrains yml specification
  -h, --help          Show this message and exit.```
```

### Hooks

The `TddaHooks()` will **verify** a catalog dataset after loading if there is a contraints definition.
This hook is automatically registered after installation and can be disabled by updating `DISABLE_HOOKS_FOR_PLUGINS`
in the `settings.py` of your `kedro` project

```python
DISABLE_HOOKS_FOR_PLUGINS = ("kedro_tdda",)
```
