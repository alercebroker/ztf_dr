import pandas as pd

from unittest import TestCase
from ztf_dr.extractors import *


DR5_SAMPLE = pd.read_parquet("tests/unit_tests/core/data/DR5_field_example.parquet")[:10]


class DataReleaseTest(TestCase):
    def setUp(self) -> None:
        self.dr_extractor = DataReleaseExtractor()

    def test_get_required_keys(self):
        keys = ['objectid', 'filterid', 'fieldid', 'rcid', 'objra', 'objdec', 'nepochs',
                'hmjd', 'mag', 'magerr', 'clrcoeff', 'catflags']
        self.assertIsInstance(self.dr_extractor.get_required_keys(), list)
        self.assertListEqual(self.dr_extractor.get_required_keys(), keys)

    def test_get_features_keys(self):
        keys = ['gal_b', 'gal_l', 'MHPS_ratio', 'MHPS_low', 'MHPS_high',
                'MHPS_non_zero', 'MHPS_PN_flag', 'SPM_A', 'SPM_t0', 'SPM_gamma', 'SPM_beta', 'SPM_tau_rise',
                'SPM_tau_fall', 'SPM_chi', 'SPM_C', 'Amplitude', 'AndersonDarling', 'Autocor_length', 'Beyond1Std', 'Con',
                'Eta_e', 'Gskew', 'MaxSlope', 'Mean', 'Meanvariance', 'MedianAbsDev', 'MedianBRP', 'PairSlopeTrend',
                'PercentAmplitude', 'Q31', 'Rcs', 'Skew', 'SmallKurtosis', 'Std', 'StetsonK', 'Pvar', 'ExcessVar',
                'SF_ML_amplitude', 'SF_ML_gamma', 'IAR_phi', 'LinearTrend', 'GP_DRW_sigma', 'GP_DRW_tau',
                'Multiband_period', 'PPE', 'Power_rate_1/4', 'Power_rate_1/3', 'Power_rate_1/2', 'Power_rate_2',
                'Power_rate_3', 'Power_rate_4', 'Psi_CS', 'Psi_eta', 'Harmonics_mag_1', 'Harmonics_mag_2',
                'Harmonics_mag_3', 'Harmonics_mag_4', 'Harmonics_mag_5', 'Harmonics_mag_6', 'Harmonics_mag_7',
                'Harmonics_phase_2', 'Harmonics_phase_3', 'Harmonics_phase_4', 'Harmonics_phase_5', 'Harmonics_phase_6',
                'Harmonics_phase_7', 'Harmonics_mse']
        self.assertIsInstance(self.dr_extractor.get_features_keys(), list)
        self.assertListEqual(self.dr_extractor.get_features_keys(), keys)

    def test_compute(self):
        response = self.dr_extractor.compute_features(DR5_SAMPLE)
        self.assertIsInstance(response, pd.DataFrame)
        self.assertEqual(response.shape[0], 10)
        self.assertEqual(response.shape[1], 71)


class FoldedKimTest(TestCase):
    def setUp(self) -> None:
        self.km_extractor = DRFoldedKimExtractor()
        self.period_extractor = DRPeriodExtractor()

    def test_get_required_keys(self):
        self.assertListEqual(self.km_extractor.get_required_keys(), ['hmjd', 'mag'])

    def test_bad_compute(self):
        with self.assertRaises(Exception) as context:
            self.km_extractor.compute_features(DR5_SAMPLE)
        self.assertRaises(Exception, context)
        self.assertTrue("First execute DRPeriodExtractor for get periods" in str(context.exception))

    def test_compute(self):
        sample = DR5_SAMPLE.reset_index()
        periods = self.period_extractor.compute_features(sample)
        periods.index = sample.index
        response = self.km_extractor._compute_features(sample, features=periods)
        self.assertIsInstance(response, pd.DataFrame)
        self.assertEqual(response.shape[0], 10)
        self.assertEqual(response.shape[1], 2)


class GalacticCoordsTest(TestCase):
    def setUp(self) -> None:
        self.gc_extractor = DRGalacticCoordinates()

    def test_get_required_keys(self):
        self.assertListEqual(self.gc_extractor.get_required_keys(), ['objra', 'objdec'])

    def test_compute(self):
        response = self.gc_extractor.compute_features(DR5_SAMPLE)
        self.assertIsInstance(response, pd.DataFrame)
        self.assertEqual(response.shape[0], 10)
        self.assertEqual(response.shape[1], 2)


class GPDRWTest(TestCase):
    def setUp(self) -> None:
        self.gpdrw_extractor = DRGPDRWExtractor()

    def test_get_required_keys(self):
        keys = ['hmjd', 'mag', 'magerr']
        self.assertListEqual(self.gpdrw_extractor.get_required_keys(), keys)

    def test_compute(self):
        response = self.gpdrw_extractor.compute_features(DR5_SAMPLE)
        self.assertIsInstance(response, pd.DataFrame)
        self.assertEqual(response.shape[0], 10)
        self.assertEqual(response.shape[1], 2)


class PowerRatePeriodTest(TestCase):
    def setUp(self) -> None:
        self.prp_extractor = DRPowerRateExtractor()

    def test_bad_compute(self):
        with self.assertRaises(Exception) as context:
            self.prp_extractor.compute_features(DR5_SAMPLE)
        self.assertRaises(Exception, context)
        self.assertTrue("First execute DRPeriodExtractor for get periodograms" in str(context.exception))

