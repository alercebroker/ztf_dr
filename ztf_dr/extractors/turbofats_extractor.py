import pandas as pd

from lc_classifier.features import TurboFatsFeatureExtractor
from typing import List
from ztf_dr.extractors.base import DR_base


class DRTurboFatsExtractor(DR_base, TurboFatsFeatureExtractor):
    def __init__(self):
        super().__init__()
        self.feature_space.data_column_names = ["mjd", "mag", "magerr"]

    def get_required_keys(self) -> List[str]:
        return ["hmjd", "mag", "magerr"]

    def _compute(self, light_curve: pd.Series, **kwargs) -> pd.Series:
        mag = light_curve["mag"]
        magerr = light_curve["magerr"]
        hmjd = light_curve["hmjd"]
        oid = light_curve.name
        df = pd.DataFrame({
            "mag": mag,
            "magerr": magerr,
            "mjd": hmjd
        })
        df["index"] = oid
        df.set_index("index", inplace=True)
        object_features = self.feature_space.calculate_features(df)
        if len(object_features) == 0:
            return self.nan_series()
        return pd.Series(object_features.squeeze())