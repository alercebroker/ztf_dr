import numpy as np
import pandas as pd

from astropy.coordinates import SkyCoord
from lc_classifier.features import GalacticCoordinatesExtractor
from typing import List
from ztf_dr.extractors.base import DR_base


class DRGalacticCoordinates(DR_base, GalacticCoordinatesExtractor):
    def __init__(self):
        super(DR_base, self).__init__()
        super(DRGalacticCoordinates, self).__init__()

    def get_required_keys(self) -> List[str]:
        return ["objra", "objdec"]

    def _compute(self, light_curve, **kwargs) -> pd.Series:
        coordinates = SkyCoord(
            ra=light_curve['objra'],
            dec=light_curve["objdec"],
            frame="icrs",
            unit="deg")
        galactic = coordinates.galactic
        np_galactic = np.stack((galactic.b.degree, galactic.l.degree), axis=-1)
        galactic_coordinates = pd.Series(np_galactic, index=["gal_b", "gal_l"])
        return galactic_coordinates
