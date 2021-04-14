import pandas as pd

from typing import List
from unittest import TestCase
from ztf_dr.extractors import *


DR5_SAMPLE = pd.read_parquet("../data/DR5_field_example.parquet")


class BaseTest(TestCase):
    def setUp(self) -> None:
        self.base = DR_base()

    def test_get_required_keys(self):
        self.assertIsInstance(self.base.get_required_keys(), List[str])
        self.assertListEqual(self.base.get_required_keys(), ['hmjd', 'mag', 'magerr'])

    def test_compute(self):
        pass


class DataReleaseTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_compute(self):
        pass


class FoldedKimTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_compute(self):
        pass


class GalacticCoordsTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_compute(self):
        pass


class GPDRWTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_compute(self):
        pass


class HarmonicsTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_compute(self):
        pass


class MHPSTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_compute(self):
        pass


class PowerRatePeriodTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_compute(self):
        pass


class SNPMTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_compute(self):
        pass


class TurboFatsTest(TestCase):
    def setUp(self) -> None:
        pass

    def test_compute(self):
        pass
