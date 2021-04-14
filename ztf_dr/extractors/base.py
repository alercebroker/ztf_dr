import pandas as pd

from abc import ABC
from lc_classifier.features.core.base import FeatureExtractor
from typing import List


class DR_base(FeatureExtractor, ABC):
    def get_required_keys(self) -> List[str]:
        return ['hmjd', 'mag', 'magerr']

    def _get_periodogram(self, objectid: int, **kwargs) -> pd.Series:
        partial_features = None
        if "features" in kwargs.keys():
            partial_features = kwargs["features"]
            if not isinstance(partial_features, pd.DataFrame):
                raise Exception("")
        else:
            raise Exception("First execute DRPeriodExtractor for get periodograms")

        cols = ["_freq", "_per"]
        for col in cols:
            if col not in partial_features.columns:
                raise ValueError(f"{col} not exists yet in partial features")
        periodogram = partial_features.loc[objectid][cols]
        periodogram = periodogram.rename({"_freq": "freq", "_per": "per"})
        return periodogram

    def _get_period(self, objectid: int, **kwargs) -> pd.Series:
        partial_features = None
        if "features" in kwargs.keys():
            partial_features = kwargs["features"]
            if not isinstance(partial_features, pd.DataFrame):
                raise Exception("")
        else:
            raise Exception("First execute DRPeriodExtractor for get periods")

        if "Multiband_period" not in partial_features.columns:
            raise ValueError("Multiband_period not exists yet in partial features")

        period = partial_features.loc[objectid]["Multiband_period"]
        return period

    def _compute(self, light_curve: pd.DataFrame, **kwargs) -> pd.Series:
        raise NotImplementedError('_compute is an abstract method')

    def _compute_features(self, detections: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return detections.apply(self._compute, axis=1, **kwargs)
