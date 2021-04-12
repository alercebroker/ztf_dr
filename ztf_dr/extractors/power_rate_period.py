from lc_classifier.features.extractors import PowerRateExtractor
from ztf_dr.extractors.base import DR_base


class DRPowerRateExtractor(DR_base, PowerRateExtractor):
    def __init__(self):
        super(DRPowerRateExtractor, self).__init__()

