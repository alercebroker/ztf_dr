import numpy as np
import pandas as pd

from P4J import MultiBandPeriodogram
from typing import List
from ztf_dr.extractors.base import DR_base


class DRPeriodExtractor(DR_base):
    def __init__(self):
        super().__init__()
        self.periodogram_computer = MultiBandPeriodogram(method='MHAOV')

    def get_required_keys(self) -> List[str]:
        return ["objectid", "filterid", "nepochs", "hmjd", "mag", "magerr"]

    def get_features_keys(self) -> List[str]:
        return ['Multiband_period', 'PPE']

    def _compute(self, light_curve, **kwargs) -> pd.Series:
        band = light_curve["filterid"]
        fids = np.full(light_curve["nepochs"], band)
        self.periodogram_computer.set_data(
            mjds=light_curve['hmjd'],
            mags=light_curve['mag'],
            errs=light_curve['magerr'],
            fids=fids)

        try:
            self.periodogram_computer.frequency_grid_evaluation(fmin=1e-3, fmax=20.0, fresolution=1e-3)
            self.frequencies = self.periodogram_computer.finetune_best_frequencies(n_local_optima=10, fresolution=1e-4)
        except TypeError as e:
            object_features = self.nan_series()
            return object_features

        best_freq, best_per = self.periodogram_computer.get_best_frequencies()
        freq, per = self.periodogram_computer.get_periodogram()
        period_candidate = 1.0 / best_freq[0]

        best_freq_band = self.periodogram_computer.get_best_frequency(band)

        # Getting best period
        best_period_band = 1.0 / best_freq_band

        # Calculating delta period
        delta_period_band = np.abs(period_candidate - best_period_band)

        # Significance estimation
        entropy_best_n = 100
        top_values = np.sort(per)[-entropy_best_n:]
        normalized_top_values = top_values + 1e-2
        normalized_top_values = normalized_top_values / np.sum(normalized_top_values)
        entropy = (-normalized_top_values * np.log(normalized_top_values)).sum()
        significance = 1 - entropy / np.log(entropy_best_n)

        object_features = {
            "Multiband_period": period_candidate,
            "PPE": significance,
            "_freq": freq,
            "_per": per
        }
        return pd.Series(object_features)
