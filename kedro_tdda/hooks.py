import logging

from kedro.framework.hooks import hook_impl
from pandas import DataFrame as  pd_DataFrame

class TddaHooks():
    @property
    def _logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        return logger

    @hook_impl
    def after_dataset_loaded(self, dataset_name: str, data: pd_DataFrame) -> None: # works only during pipeline, not during kedro tdda
        pass

tdda_hooks = TddaHooks()
