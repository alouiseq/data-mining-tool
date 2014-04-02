# -*- coding: utf-8 -*-
"""Tests for parsing run group config."""

from google3.testing.pybase import googletest
from google3.cityblock.special.legacy import parse_run_groups_config


class ParseTests(googletest.TestCase):

  def _AssertFloatsEqual(self, actual, expected, max_delta=0.001):
    self.assertTrue(abs(expected-actual) < max_delta)

  def _ExpectMid(self, lat1, lon1, lat2, lon2, expected_lat, expected_lon):
    actual_lat, actual_lon = parse_run_groups_config.Mid(lat1, lon1, lat2, lon2)
    self._AssertFloatsEqual(actual_lat, expected_lat)
    self._AssertFloatsEqual(actual_lon, expected_lon)

  def testMid(self):
    """Test to check accuracy of calculating midpoint from coordinates."""
    self._ExpectMid(49.3904953, -23.3949491, 52.1293459, -26.9301945,
                    50.7599206, -25.1625718)
    self._ExpectMid(64.1953647, -10.5409682, 62.5829053, -11.4910591,
                    63.3891350, -11.0160137)

  def testParseRunGroupsConfig(self):
    """Test to check the accuracy of parsing mechanism.

    collections is a list of objects containing country code, site name,
    latitude, longitude, and runs.
    """
    lines = ("""This portion is a test and should be ignored if parsing
              is done correctly...

              run_group: "IL,PS-DavidGarden"
              run_group: "US-DoNotLaunch_NotEnoughData_209230_L19200"
              # AA-NotToBeIncluded "Ignore this"
              # (00.0000000, -00.0000000)
              run_group <
                name: "Ignore this"
                run: "00000000_000000_L00000"
              \n# AQ

              # AQ-HalfMoonIslandAQ "Half Moon Island, Antarctica"
              # (-62.5965443,-59.9072398) -- (-62.5935121,-59.8935582)
                run_group <
                name: "AQ-HalfMoonIslandAQ"
                run: "20100125_015702_panos_antartica_bam"
              >


              # AT

              # AT-Ischgi "NA"
              # (46.9419651,10.2812022) -- (47.0107649,10.3410778)
              run_group <
                name: "AT-Ischgi"
                run: "20110330_213813_L19069"
                run: "20110331_071816_L19069"
                run: "20110331_214359_L19069"
                run: "20110331_220352_L19069"
                run: "20110401_212830_L19069"
                run: "20110401_231023_L19069"
                run: "20110401_235438_L19069"

              XX-OrphanRuns

              # ZZ-ExcludeMe "Exclude Me"
              # (99.9999999, -99.9999999) -- (99.9999999, -99.9999999)
              run_group <
                name: "ExcludeMe"
                run: "99999999_999999_L99999
                run: "88888888_888888_L88888
              This last portion shoud also be ignored by the parser
              >""")

    lines = lines.split('\n')
    collections = parse_run_groups_config.ParseRunGroupsConfig(lines)

    # ensure only the allowed data is extracted
    self.assertEqual(2, len(collections))

    # test country code for first and last entries
    self.assertEqual('AQ', collections[0].country_code)
    self.assertEqual('AT', collections[1].country_code)

    # test site name for first and last entries
    self.assertEqual('HalfMoonIslandAQ', collections[0].issue_name)
    self.assertEqual('Ischgi', collections[1].issue_name)

    # test latitude and longitude for first and last entries
    self._AssertFloatsEqual(collections[0].lat, -62.5950282)
    self._AssertFloatsEqual(collections[0].lon, -59.900399)
    self._AssertFloatsEqual(collections[1].lat, 46.976365)
    self._AssertFloatsEqual(collections[1].lon, 10.31114)

    # test runs for first and last entries
    self.assertEqual(['20100125_015702_panos_antartica_bam'],
                     collections[0].runs)
    self.assertEqual(['20110330_213813_L19069', '20110331_071816_L19069',
                      '20110331_214359_L19069', '20110331_220352_L19069',
                      '20110401_212830_L19069', '20110401_231023_L19069',
                      '20110401_235438_L19069'], collections[1].runs)

  def testNonAscii(self):
    """Test for non-ascii unicode characters."""
    lines = """# AQ
               # CL-ViñaViuManent "Viña Viu Manent"
               # (-34.6518549,-71.3119340) -- (-34.6465851,-71.3004360)
               run_group <
                 name: "CL-ViñaViuManent"
                 run: "20120320_071319_L19119" """

    lines = lines.split('\n')
    collections = parse_run_groups_config.ParseRunGroupsConfig(lines)

    self.assertEqual('ViñaViuManent', collections[0].issue_name)


if __name__ == 'main':
  googletest.main()
