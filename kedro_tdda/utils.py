from pathlib import Path
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_paths():
    PROJECT_PATH = Path().cwd()
    TDDA_TARGET_DIR = PROJECT_PATH / 'conf' / 'base' / 'tdda'
    return PROJECT_PATH, TDDA_TARGET_DIR

def get_catalog(env):
    PROJECT_PATH = get_paths()[0]
    # load kedro catalog & names
    bootstrap_project(PROJECT_PATH)
    with KedroSession.create(
        project_path=PROJECT_PATH,
        env=env,
    ) as session:
        context = session.load_context()
        catalog = context.catalog
    return catalog

def retain_pandas_only(catalog):
    catalog_entries = catalog.list()
    catalog_entries = [x for x in catalog_entries if str(type(catalog._datasets[x])).find('kedro_datasets.pandas') > -1 ]
    return catalog_entries

def retain_single_df(catalog_entries, dataset_name):
    assert dataset_name in catalog_entries, f"{dataset_name} is not matching a kedro_datasets.pandas specification. Check for typos or catalog specification"
    catalog_entry = [x for x in catalog_entries if x == dataset_name]
    return catalog_entry

def get_name_tddapath_dict(env, dataset_name):

    # set helper paths
    PROJECT_PATH, TDDA_TARGET_DIR = get_paths()

    # only at discovery, check if exists
    if not TDDA_TARGET_DIR.is_dir():
        TDDA_TARGET_DIR.mkdir()    

    catalog = get_catalog(env)
    catalog_entries = retain_pandas_only(catalog)

    # if dataset_name is supplied, filter to only those entries
    if dataset_name:
        catalog_entries = retain_single_df(catalog_entries, dataset_name)
    
    return {x : TDDA_TARGET_DIR / f"{x}.yml" for x in catalog_entries}

def formatted_log_verification(entry, verification):
    
    if verification.failures > 0:

        failed_fields_chars = [(field,check_name) for field in verification.fields for check_name, passed in verification.fields[field].items() if not passed]
        failed_fields_chars = "\n".join([ "✗ " + ": ".join(x) for x in failed_fields_chars])

        error_str = [
            f"Verification for `{entry}` failed: {verification.passes} passses, {verification.failures} failures:",
            f"{failed_fields_chars}",
        ]
        
        logger.error("\n".join(error_str))
    else:
        logger.info(f"Verification summary `{entry}`: {verification.passes} passses, {verification.failures} failures")