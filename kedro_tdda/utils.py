import json
import logging
from pathlib import Path

import yaml
from kedro.framework.session import KedroSession
from kedro.io import DatasetError
from tdda.constraints import detect_df, discover_df, verify_df
from tdda.constraints.base import Verification

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class KedroSettings:
    """
    Convenience class for easy passthrough of environment-specific kedro 
    project settings to utility function of tdda: discover, verify & detect
    """    
    def __init__(self, env: str):
        project_path = Path.cwd()
        session = KedroSession.create(project_path=project_path, env=env)
        self.context = session.load_context()

        self.conf_source = Path(self.context.config_loader.conf_source)
        self.tdda_dir = self.conf_source / env / "tdda"

        if not self.tdda_dir.is_dir():
            self.tdda_dir.mkdir()

        self.catalog = self.context.catalog
        self.catalog_entries_pd = [
            x
            for x in self.catalog.list()
            if str(type(self.catalog._datasets[x])).find("kedro_datasets.pandas") > -1
        ]


class TddaVerificationError(Exception):
    """
    Custom error when tdda verify results in an error

    Args:
        Exception (TddaVerificationError): failure when dataframe
            deviates from specified constraints
    """    
    def __init__(self, msg):
        self.msg = msg

def discover_constraints(dataset_name: str, overwrite: bool, ks: KedroSettings) -> None:
    """Utility function to discover tdda constrains for one or more pandas datasets

    Args:
        dataset_name (str): dataset name as specified in catalog
        overwrite (bool): indicates whether to overwrite the tdda constraint specification
        ks (KedroSettings): utility object for passing environment-specific kedro settings
    """    
    try:
        df = ks.catalog.load(dataset_name)
    except DatasetError:
        logger.warning(f"Failed to load {dataset_name} from catalog.")
        return None

    constraints = discover_df(df)
    tdda_path = ks.tdda_dir / f"{dataset_name}.yml"
    if not overwrite and tdda_path.exists():
        logger.warning(
            f"TDDA discovery for `{dataset_name}` skipped. File exists: ./{tdda_path.relative_to(ks.conf_source.parent)}"
        )
    else:
        with open(tdda_path, "w") as file:
            output_dict = json.loads(json.dumps(constraints.to_dict()))
            yaml.safe_dump({dataset_name: output_dict}, file, indent=4)
        logger.info(
            f"TDDA constraints are written to ./{tdda_path.relative_to(ks.conf_source.parent)}"
        )


def verify_constraints(dataset_name: str, ks: KedroSettings) -> None:
    """Utility function to verify tdda constrains for one or more pandas datasets

    Args:
        dataset_name (str): dataset name as specified in catalog
        ks (KedroSettings): utility object for passing environment-specific kedro settings
    """    
    constraints = ks.context.config_loader["tdda"].get(dataset_name)

    if not constraints:
        logger.warning(f"No constraints found for {dataset_name}")
    else:
        df = ks.catalog.load(dataset_name)
        verification = verify_df(df, constraints)
        formatted_log_verification(dataset_name, verification)


def detect_issues(dataset_name: str, ks: KedroSettings, target_dir: str) -> None:
    """Utility function to detect tdda constrains for one or more pandas datasets

    Args:
        dataset_name (str): dataset name as specified in catalog
        ks (KedroSettings): utility object for passing environment-specific kedro settings
        target_dir (str): directory in which the output files for detection should be saved
    """    
    target_dir = Path(target_dir)
    if not target_dir.is_dir():
        target_dir.mkdir()
    detection_target_file = target_dir / f"{dataset_name}.csv"

    constraints = ks.context.config_loader["tdda"].get(dataset_name)

    if not constraints:
        logger.warning(f"No constraints found for {dataset_name}")
    else:
        df = ks.catalog.load(dataset_name)
        verification = detect_df(
            df,
            constraints_path=constraints,
            outpath=str(detection_target_file),
            per_constraint=True,
        )
        if verification.failures > 0:
            logger.info(f"Detection for {dataset_name} written to ./{detection_target_file}")
        formatted_log_verification(dataset_name, verification, error = False)

def formatted_log_verification(entry:str, verification: Verification, error=True) -> None:
    """
    Utility function to format data validation errors in the output

    Args:
        entry (str): dataset name as specified in catalog
        verification (Verification): tdda verification object
        error (bool, optional): indicator if an error should be raised. 
            Defaults to True. The False condition is useful when running
            `kedro tdda detect`. In that case, files with anomalies will be 
            saved without interruption, the CLI will return warnings instead
            of Exceptions

    Raises:
        TddaVerificationError: failure in case catalog dataset deviates
            from the tdda constraint specification
    """
    if verification.failures > 0:
        failed_fields_chars = [
            (field, check_name)
            for field in verification.fields
            for check_name, passed in verification.fields[field].items()
            if not passed
        ]
        failed_fields_chars = "\n".join(
            ["✗ " + ": ".join(x) for x in failed_fields_chars]
        )

        msg = f"Dataset `{entry}` deviates from constraint specification:\n{failed_fields_chars}"
        if error:
            raise TddaVerificationError(msg)
        else:
            logger.warning(msg)
    else:
        logger.info(
            f"Verification summary `{entry}`: {verification.passes} passses, {verification.failures} failures"
        )
