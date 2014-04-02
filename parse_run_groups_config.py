"""Data mine special-reports for legacy collections.

Mine data from special-reports legacy website to extract site information
along with run groups.
"""

import re


class LegacyIssue(object):
  def __init__(self, issue_name, country_code):
    self.country_code = country_code
    self.issue_name = issue_name
    self.lat = None
    self.lon = None
    self.runs = []

  def __str__(self):
    return '{}-{} {},{} {}'.format(
        self.country_code, self.issue_name,
        self.lat, self.lon, self.runs)


def Mid(lat1, lon1, lat2, lon2):
  """Calculates midpoint of two sets of latitudes and longitudes.

  Legacy collections website displays two sets of coordinates where the
  midpoint of the two is the accurate location of the site.

  Args:
    lat1: First latitude coordinate.
    lon1: First longitude coordinate.
    lat2: Second latitude coordinate.
    lon2: Second longitude coordinate.

  Returns:
    The actual location of the site.
  """
  lat = (float(lat1) + float(lat2)) / 2
  lon = (float(lon1) + float(lon2)) / 2
  return lat, lon


def ParseRunGroupsConfig(lines):
  """This is a list of objects containing site information.

  This will return a list of objects where each object contains the
  country code, site name, coordinates, and runs.

  Args:
    lines: list of lines of data to be parsed.

  Returns:
    list of LegacyIssue objects.
  """
  while not lines[0].startswith('# AQ'):
    lines.pop(0)

  country_code = None
  issue_name = None
  issue = None
  skip_files = False
  collection_name_pattern = re.compile(r'^\s*# ([A-Z][A-Z])-([^ ]*)'
                                       r' "([^"]*)"\s*$')
  lat_lng_pattern = re.compile(r'^\s*# \(([-0-9.]+),([-0-9.]+)\)'
                               r' -- \(([-0-9.]+),([-0-9.]+)\)\s*$')
  run_pattern = re.compile(r'^\s*run: "([^"]*)"\s*$')
  issues = []

  for line in lines:
    match = collection_name_pattern.match(line)
    if match:
      skip_files = False
      country_code = match.group(1)
      issue_name = match.group(2)
      
      # separate site name into words for readability
      buffer = []
      for s in re.split(r'([A-Z][A-Z]*[a-z0-9-]*)', issue_name):
        if s: 
          buffer.append(s)
      issue_name = ' '.join(buffer)
      
      if ('test' not in issue_name.lower() and '_' not in issue_name
          and '~' not in issue_name):
        issue = LegacyIssue(issue_name, country_code)
        issues.append(issue)
      else:
        skip_files = True
        issue = None

    if not skip_files:
      match = lat_lng_pattern.match(line)
      if match:
        lat1, lon1, lat2, lon2 = match.groups()
        # finds midpoint
        issue.lat, issue.lon = Mid(lat1, lon1, lat2, lon2)

      match = run_pattern.match(line)
      if match and issue:
        run = match.group(1)
        issue.runs.append(run)

    if 'XX-OrphanRuns' in line:
      break
  return issues


def main():
  with open('/home/cb-ops-sys/www/special/legacy/reports/run_groups.config',
            'rU') as f:
    lines = f.readlines()
  results = ParseRunGroupsConfig(lines)

  print '\n'.join([str(r) for r in results])


if __name__ == '__main__':
  main()

