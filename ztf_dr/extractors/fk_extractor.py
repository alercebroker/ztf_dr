from lc_classifier.features import FoldedKimExtractor
from typing import List
from ztf_dr.extractors.base import DR_base


class DRFoldedKimExtractor(DR_base, FoldedKimExtractor):
    def __init__(self):
        super.__init__()

    def get_required_keys(self) -> List[str]:
        return ['hmjd', 'mag']
