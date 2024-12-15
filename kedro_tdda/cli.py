import logging

import click

from .utils import (
    KedroSettings,
    detect_issues,
    discover_constraints,
    verify_constraints,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_ENV_TDDA = "base"
DEFAULT_DIR_TDDA_DETECT = "./tdda_detect"


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
    default=DEFAULT_ENV_TDDA,
    help="Kedro configuration environment name. Defaults to `base`.",
)
@click.option(
    "--overwrite",
    "-o",
    help="Boolean indicator for overwriting an existing tdda constrains yml specification",
    is_flag=True,
)
def discover(dataset_name: str, env: str, overwrite: bool):
    """
    The tdda discover command generates constraints for data, 
    and saves the generated constraints as a yaml file.
    """
    ks = KedroSettings(env)

    if dataset_name:
        discover_constraints(dataset_name, overwrite=overwrite, ks=ks)
    else:
        for x in ks.catalog_entries_pd:
            discover_constraints(dataset_name=x, overwrite=overwrite, ks=ks)


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
    default=DEFAULT_ENV_TDDA,
    help="The kedro environment where the dataset to retrieve is available. Default to 'base'",
)
def verify(
    dataset_name: str,
    env: str,
):
    """
    The tdda verify command is used to validate pandas dataframes, 
    against a constraints specification.
    """    
    ks = KedroSettings(env)

    if dataset_name:
        verify_constraints(dataset_name, ks=ks)
    else:
        for x in ks.catalog_entries_pd:
            verify_constraints(dataset_name=x, ks=ks)


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
    default=DEFAULT_ENV_TDDA,
    help="The kedro environment where the dataset to retrieve is available. Default to 'base'",
)
@click.option(
    "--target-dir",
    "-t",
    default=DEFAULT_DIR_TDDA_DETECT,
    help="Target directory in which the tdda detection csv can be saved. Should always be a directory",
)
def detect(dataset_name: str, env: str, target_dir: str):
    """
    The tdda detect command is used to detect anomalies on data, 
    by checking pandas dataframes against specified constraints.
    """    
    ks = KedroSettings(env)

    if dataset_name:
        detect_issues(dataset_name, ks=ks, target_dir=target_dir)
    else:
        for x in ks.catalog_entries_pd:
            detect_issues(dataset_name=x, ks=ks, target_dir=target_dir)
