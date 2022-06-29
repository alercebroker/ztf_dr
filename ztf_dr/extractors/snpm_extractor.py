import numpy as np
import pandas as pd

from typing import Tuple
from lc_classifier.features.extractors.sn_parametric_model_computer import SPMExtractorDR, SNModelScipyDR
from lc_classifier.features.extractors.sn_parametric_model_computer import mag_to_flux
from ztf_dr.extractors.base import DR_base


class DRSNPMExtractor(DR_base, SPMExtractorDR):
    def __init__(self):
        super(DR_base, self).__init__(bands=[0])
        super(SPMExtractorDR, self).__init__(bands=[0])
        self.sn_model = SNModelScipyDR()

    def get_features_keys(self) -> Tuple[str, ...]:
        return self.get_features_keys_without_band()

    def _compute(self,  light_curve, **kwargs) -> pd.Series:
        times = light_curve["hmjd"]
        times = times - np.min(times)

        mag_targets = light_curve["mag"]
        targets = mag_to_flux(mag_targets)
        errors = light_curve["magerr"]
        errors = mag_to_flux(mag_targets - errors) - targets
        times = times.astype(np.float32)
        targets = targets.astype(np.float32)
        targets = targets - np.percentile(targets, 5)
        fit_error = self.sn_model.fit(times, targets, errors)
        model_parameters = self.sn_model.get_model_parameters()
        model_parameters.append(fit_error)
        response = pd.Series(data=model_parameters, index=self.get_features_keys_without_band())
        return response
