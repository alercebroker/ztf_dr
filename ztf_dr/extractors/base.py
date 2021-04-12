from abc import ABC

from lc_classifier.features.core.base import FeatureExtractor
from typing import List
import pandas as pd


class DR_base(FeatureExtractor, ABC):
    def get_required_keys(self) -> List[str]:
        return ['hmjd', 'mag', 'magerr']

    def _compute(self, light_curve) -> pd.Series:
        raise NotImplementedError('_compute is an abstract method')

    def _compute_features(self, detections: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return detections.apply(self._compute, axis=1)
