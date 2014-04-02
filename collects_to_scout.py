"""This program bulk imports site and collection data to Scout.

Relevant information are extracted from spreadsheets (or legacy collections),
packages data into a protocol buffer, and invokes the RPC system
(Stubby Interface) to import sites and collections to Scout.
"""

import csv
import json
import re
import sys
import urllib

from google3.cityblock.special.legacy import parse_run_groups_config
from google3.cityblock.special.workflow.client import scout_client
from google3.cityblock.special.workflow.proto import scout_pb2


class CurrentIssue(object):
  def __init__(self, issue_name, address):
    self.issue_name = issue_name
    self.address = address
    self.lat = None
    self.lon = None
    self.method = []
    self.runs = []

  def __str__(self):
    return '{}, {}, {}, {}, {}'.format(
        self.issue_name, self.method, self.address,
        self.lat, self.lon, self.runs)


def Parser(arg_file):
  """Parses a spreadsheet for relevent data.

  Parses through a csv file (spreadsheet) and extracts required information
  for importing to Scout.

  Args:
    arg_file: CSV file containing all collection information for a particular
      country.

  Returns:
    List of sites objects
  """
  sites = []

  with open(arg_file, 'rb') as csvfile:
    readme = csv.DictReader(csvfile, fieldnames=None)

    for row in readme:
      # print row
      issue_name = row['Location Name'].strip()
      method = row['Equipment'].strip()
      address = row['Address'].strip()
      lat, lon = Geocode(address)

      site = CurrentIssue(issue_name, address)
      site.method = method
      site.lat = lat
      site.lon = lon
      sites.append(site)

  # sort the list of site objects based on the site name
  sites = sorted(sites, key=lambda CurrentIssue: CurrentIssue.issue_name)

  return sites


def ToCSV(arg_file, sites):
  """Write to a new csv file comprising all required fields.

  Create a new csv file containing site and collection information to be
  imported to Scout.

  Args:
    arg_file: CSV file of collections specific to country where the name is
      modified to create new csv file name.
    sites: List of site objects
  """
  name_change = re.sub(r'(\w+).csv', r'\1_toScout.csv', arg_file)

  out_file = open(name_change, 'w')

  # Write headers for site data
  headers = 'SITE, METHOD, ADDRESS, LATITUDE, LONGITUDE, RUNS'
  out_file.write(headers + '\n')

  # Write site data into a comma separated string
  for site in sites:
    site_data = str(site)
    site_data += '\n'
    out_file.write(site_data)

  out_file.close()


def Merger(current_sites, legacy_sites):
  """Combine sites' data from two different sources.

  Find data from two list of site objects from csv files and legacy collections
  and merge indentical sites with necessary information (site name, location,
  method, and runs).

  Args:
    current_sites: List of site objects containing data from spreadsheets.
    legacy_sites: List of site objects containing legacy collections.

  Returns:
    List of merged site objects.
  """
  match = False

  # merge runs from legacy sites to current site objects
  for site in current_sites[:]:
    for legacy_data in sorted(legacy_sites,
                              key=lambda CurrentIssue: CurrentIssue.issue_name):
      if site.issue_name == legacy_data.issue_name:
        site.runs.extend(legacy_data.runs)
        match = True
      # no match found, just add site object to the list
      if not match:
        current_sites.append(legacy_data)
      else:
        match = False

  return sorted(current_sites,
                key=lambda CurrentIssue: CurrentIssue.issue_name)


def CreateSite(site, is_legacy):
  """Create a new site.

  The following functions invoke functions that use the Stubby interface
  to create a site and import to Scout.

  Args:
    site: site object containing site information.
    is_legacy: boolean value whether data is is legacy data.

  Returns:
    New site id, SiteProto object, and Scout Datastore object.
  """
  new_site = scout_pb2.SiteProto()

  site_name = new_site.metadata.name.localized_string.add()
  site_name.translation = site.issue_name
  site_name.locale = 'en-US'   # default to en-US locale

  new_site.metadata.lat = site.lat
  new_site.metadata.lng = site.lon

  if is_legacy:   # LAUNCHED
    new_site.metadata.state = site.CLOSED
  else:   # NOT LAUNCHED
    new_site.metadata.state = site.ACTIVE

  # building = new_site.metadata.building.add()
  # bldg_name = building.name   #building name-TranslatedStringProto (optional)

  # level = building.level.add()
  # level.order = changeme  # -1 for basement, 0 for ground, etc.

  # level.name.primary_locale = 'en-US'
  # local_name = level.name.localized_string.add()
  # local_name.translation = "changeme"   # (ex. F1 or Floor 1)
  # local_name.locale = 'en-US'

  # level.abbreviation.primary_locale = 'en-US'
  # local_abbr = level.abbreviation.localized_string.add()
  # local_abbr.translation = changeme  # (ex. 1 for ground floor)
  # local_abbr.locale = 'en-US'

  scout_datastore_obj = scout_client.NewStubbyScoutDatastore()
  site_id = scout_datastore_obj.CreateSite(new_site)

  return site_id, new_site, scout_datastore_obj


def CreateCollection(site_id, new_site, scout_datastore_obj,
                     site, is_legacy):
  """Create a new collection for the site.

  The new collection shall include the type of equipment
  used to obtain the collection currently set to only
  have one method (e.g. Car, Trike, Trolley, etc.).

  Args:
    site_id: Site id.
    new_site: SiteProto object.
    scout_datastore_obj: Scout Datastore object.
    site: site object containing site data.
    is_legacy: State of collection

  Returns:
    New collection id and CollectionProto object.
  """
  collection = new_site.metadata.collection.add()
  collect_method = collection.metadata.method.add()
  collect_method = site.method.upper()

  if is_legacy:   # LAUNCHED
    collection.metadata.state = new_site.metadata.collection.LAUNCHED
  else:   # NOT LAUNCHED
    if site.runs:   # runs are uploaded so collection has been collected
      if site.method == 'CAR' or 'TRIKE' or 'SNOWMOBILE' or 'TREKKER':
        collection.metadata.state = (
            (new_site.metadata.collection.GEOMETRY_REVIEW_COMPLETE))
      else:   # TROLLEY
        collection.metadata.state = (
            (new_site.metadata.collection.HOUSECAT_COMPLETE))
    else:  # NEED TO MANUALLY CHECK COLLECTION
      collection.metadata.state = new_site.metadata.collection.COLLECTED

  collection_id = scout_datastore_obj.CreateCollection(site_id, collection)
  return collection_id, collection


def WriteRunGroups(new_collect, scout_datastore_obj, site):
  """Create a new run group for the runs.

  Create a run group to include all the runs associated with a collection
  but is not necessary to import to Scout.

  Args:
    new_collect: CollectionProto object.
    scout_datastore_obj: Scout datastore object.
    site: site object containing site data.

  Returns:
    Status of run group creation??
  """
  run_group = scout_pb2.RunGroupProto()

  for run_id in site.runs:
    # run = new_collect.metadata.run.add()
    # run.id = run_id
    run = run_group.run_id.add()
    run = run_id

  status = scout_datastore_obj.WriteRunGroups(run_group)
  return status


def Geocode(address):
  """Uses Google API to geocode site addresses.

  Many site information only include the address, so geocoding is used
  to convert addresses to coordinates.

  Args:
    address: The address to convert to latitude and longitude.

  Returns:
    Latitude and longitude or empty lat and lon if no address is present.
  """
  if not address:
    return None, None

  url = ('http://maps.googleapis.com/maps/api/geocode/json?address=%s&'
         'sensor=false' % address)
  google_response = urllib.urlopen(url)
  json_response = json.loads(google_response.read())
  # for s in json_response['results']:
  s = json_response['results'][0]
  latlong = s['geometry']['location']
  lat = latlong['lat']
  lon = latlong['lng']

  return lat, lon


def main():
  args = sys.argv[1:]
  is_legacy = False

  if not args:
    print 'usage: [--legacy] [--write csvFile1 ...]'
    sys.exit()
  else:
    if args[0] == '--legacy':
      is_legacy = True

  if is_legacy:
    args = args[1:]
    # extract site and runs from legacy collects
    with open('/home/cb-ops-sys/www/special/legacy/reports/run_groups.config',
              'rU') as f:
      lines = f.readlines()
    legacy_results = parse_run_groups_config.ParseRunGroupsConfig(lines)
    sites = legacy_results

  write_csv = False
  if args:
    if args[0] == '--write':
      write_csv = True
      args = args[1:]
      if not args:
        print 'No CSV file to write to'
        sys.exit()

  for arg_file in args:
    current_sites = Parser(arg_file)
    if is_legacy:
      all_sites = Merger(current_sites, legacy_results)
      sites = all_sites
    else:
      sites = current_sites
    if write_csv:
      # Write site information to csv file
      ToCSV(arg_file, sites)

  # test printing
  print '\n'.join([str(r) for r in sites])

  """for site in sites:
    site_id, new_site, scout_datastore_obj = CreateSite((site, is_legacy)
    collection_id, new_collect = CreateCollection(
        site_id, new_site, scout_datastore_obj, site, is_legacy)
    status = WriteRunGroups(new_collection, scout_datastore_obj, site)
    print 'Status of WriteRunGroups():'
    print status
  """


if __name__ == '__main__':
  main()
  # app.run()
