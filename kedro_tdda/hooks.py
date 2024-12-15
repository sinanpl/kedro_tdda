import logging

from kedro.framework.context import KedroContext
from kedro.framework.hooks import hook_impl
from pandas import DataFrame as pd_DataFrame
from tdda.constraints import verify_df

from .utils import formatted_log_verification


class TddaHooks:
    @property
    def _logger(self) -> logging.Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        return logger

    @hook_impl
    def after_context_created(self, context: KedroContext):
        """
        After the KedroContext is created, the config loader is
        modified to include the pattern `tdda`. This will load tdda specification 
        that match the regex pattern. Subsequently, constraints specifications 
        are saved for furhter use with dataset hooks.
        """        
        context.config_loader.config_patterns['tdda'] = ["tdda/*"]
        self.tdda_constraints = context.config_loader["tdda"]

    @hook_impl
    def after_dataset_loaded(
        self, dataset_name: str, data: pd_DataFrame
    ) -> None:
        """
        This dataset hook will run tdda verify on each dataset
        that has a constraint specification 
        """        
        dataset_constraints = self.tdda_constraints.get(dataset_name)
        if dataset_constraints:
            verification = verify_df(data, dataset_constraints)
            formatted_log_verification(entry=dataset_name, verification=verification)


tdda_hooks = TddaHooks()
