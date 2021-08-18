import pandas as pd

from lc_classifier.features.core.base import FeatureExtractor
from typing import List
from ztf_dr.extractors import (DRGalacticCoordinates, DRGPDRWExtractor, DRMHPSExtractor, DRPeriodExtractor,
                               DRSNPMExtractor, DRTurboFatsExtractor, DRPowerRateExtractor, DRFoldedKimExtractor,
                               DRHarmonicsExtractor)


class DataReleaseExtractor(FeatureExtractor):
    def __init__(self):
        self.extractors = [
            # DRGalacticCoordinates(),
            # DRMHPSExtractor(),
            DRSNPMExtractor(),
            # DRTurboFatsExtractor(),
            # DRGPDRWExtractor(),
            # DRPeriodExtractor(),
            # DRPowerRateExtractor(),
            # DRFoldedKimExtractor(),
            # DRHarmonicsExtractor(),
        ]
        self.basic_data = ["filterid", "objra", "objdec", "nepochs"]

    def _init_response(self, light_curves):
        response = light_curves[self.basic_data]
        return response

    def _compute_features(self, light_curves: pd.DataFrame) -> pd.DataFrame:
        light_curves.set_index("objectid", inplace=True)
        response = self._init_response(light_curves)
        for ex in self.extractors:
            df = ex._compute_features(light_curves, features=response)
            response = response.join(df)
        partial_cols = [x for x in response.columns if x.startswith("_")]
        for col in partial_cols:
            del response[col]
        return response

    def get_required_keys(self):
        return ['objectid', 'filterid', 'fieldid', 'rcid', 'objra', 'objdec', 'nepochs',
                'hmjd', 'mag', 'magerr', 'clrcoeff', 'catflags']

    def get_features_keys(self) -> List[str]:
        features_keys = []
        for extractor in self.extractors:
            features_keys += extractor.get_features_keys()
        return features_keys
