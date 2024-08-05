import click
import json
import yaml
from pathlib import Path
from .utils import (
    get_paths, 
    get_catalog, 
    get_name_tddapath_dict,
    formatted_log_verification
)

from tdda.constraints import discover_df, verify_df, detect_df

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@click.group(name="tdda")
def commands():
    """Kedro plugin for interactions with tdda"""
    pass  # pragma: no cover

@commands.group(name="tdda")
def tdda_commands():
    """Use tdda-specific commands inside kedro project."""
    pass  # pragma: no cover

@tdda_commands.command(name="discover")
@click.option(
    "--dataset",
    "-d",
    "dataset_name",
    help="The name of the pandas catalog entry for which constraints be inferred.",
)
@click.option(
    "--env",
    "-e",
    default="base",
    help="The kedro environment where the dataset to retrieve is available. Default to 'base'",
)
@click.option(
    "--overwrite", 
    "-o",
    help='Boolean indicator for overwriting an existing tdda constrains yml specification', 
    is_flag=True
)
def discover(
    dataset_name: str, 
    env: str, 
    overwrite: bool
):
    catalog = get_catalog(env)

    # construct catalog_name:tdda paths dict
    entry_and_tdda_paths = get_name_tddapath_dict(env, dataset_name)

    # run discovery    
    # TODO: can be parralellised, with kedro --runner=Threadrunner (?)
    for entry_name, tdda_path in entry_and_tdda_paths.items():
        if not overwrite and tdda_path.exists():
            logger.warn(f"TDDA discovery for `{entry_name}` skipped. File exists: {tdda_path.relative_to(get_paths()[0])}")
        else:
            df = catalog.load(entry_name)
            constraints = discover_df(df)
            with open(tdda_path, 'w') as file:
                output_dict = json.loads(json.dumps(constraints.to_dict()))
                yaml.safe_dump(output_dict, file, indent=4)
            logger.info(f"TDDA constraints are written to ./{tdda_path.relative_to(get_paths()[0])}")

@tdda_commands.command(name="verify")
@click.option(
    "--dataset",
    "-d",
    "dataset_name",
    help="The name of the pandas catalog entry for which verification will be executed.",
)
@click.option(
    "--env",
    "-e",
    default="base",
    help="The kedro environment where the dataset to retrieve is available. Default to 'base'",
)
def verify(
    dataset_name: str, 
    env: str, 
):
    catalog = get_catalog(env)

    # construct catalog_name:tdda paths dict
    entry_and_tdda_paths = get_name_tddapath_dict(env, dataset_name)
    for entry,tdda_path in entry_and_tdda_paths.items():
        with open(tdda_path) as f:
            constraints = yaml.safe_load(f)
        df = catalog.load(entry)
        verification = verify_df(df, constraints)
        formatted_log_verification(entry, verification)


@tdda_commands.command(name="detect")
@click.option(
    "--dataset",
    "-d",
    "dataset_name",
    help="The name of the pandas catalog entry for which detection will be written.",
)
@click.option(
    "--env",
    "-e",
    default="base",
    help="The kedro environment where the dataset to retrieve is available. Default to 'base'",
)
@click.option(
    "--target-dir",
    "-t",
    default="./tdda_detect",
    help="Target directory in which the tdda detection csv can be saved. Should always be a directory",
)
def detect(
    dataset_name: str, 
    env: str, 
    target_dir: str
):
    catalog = get_catalog(env)
    entry_and_tdda_paths = get_name_tddapath_dict(env, dataset_name)
    for entry,tdda_path in entry_and_tdda_paths.items():
        with open(tdda_path) as f:
            constraints = yaml.safe_load(f)
        df = catalog.load(entry)

        # construct target path
        target_path = Path(target_dir)
        if not target_path.is_dir():
            target_path.mkdir()
        detection_target_file = target_path / f"{entry}.csv"

        verification = detect_df(
            df, 
            constraints_path=constraints, 
            outpath=detection_target_file, 
            per_constraint=True
        )
        formatted_log_verification(entry, verification)
        logger.info(f"Detection for {entry} written to {detection_target_file}")
