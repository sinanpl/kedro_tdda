import logging
from typing import Optional

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
def discover(dataset_name: Optional[str] = None, env: Optional[str] = DEFAULT_ENV_TDDA, overwrite: Optional[bool] = None):
    """
    Discover constraints for pandas datasets in the catalog.

    \f
    Args:
        dataset_name (Optional[str]): Optional catalog name for the pandas dataset. 
            If not specified, discover will write constraints for every
            pandas dataset in the catalog
        env (Optional[str]): Which conf/<env>/tdda folder to write the constraints to
        overwrite (Optional[bool]): If a constraints file exists, the overwrite flag
            will overwrite the file.
    
    Usage:
        ```sh
        kedro tdda discover
        ```
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
    dataset_name: Optional[str]=None,
    env: Optional[str]='base',
):
    """
    Verify data against constraints specifications
    
    \f
    Args:
        dataset_name (Optional[str]): Optional catalog name for the pandas dataset. 
            If not specified, verify will check constraints for every
            pandas dataset in the catalog
        env (Optional[str]): Which conf/<env>/tdda folder to read constraints from

    Usage:
        ```sh
        kedro tdda verify
        ```
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
def detect(dataset_name: Optional[str]=None, env: Optional[str]='base', target_dir: Optional[str]='./tdda_detect'):
    """
    Detect and write anomalies for data that deviates from the constraints

    \f
    Args:
        dataset_name (Optional[str]): Optional catalog name for the pandas dataset. 
            If not specified, discover will write constraints for every
            pandas dataset in the catalog
        env (Optional[str]): Which conf/<env>/tdda folder to read constraints from
        target_dir (Optional[str]): Which directory to write `detect` (.csv) files to
    
    Usage:
        ```sh
        kedro tdda discover
        ```
    """
    ks = KedroSettings(env)

    if dataset_name:
        detect_issues(dataset_name, ks=ks, target_dir=target_dir)
    else:
        for x in ks.catalog_entries_pd:
            detect_issues(dataset_name=x, ks=ks, target_dir=target_dir)
