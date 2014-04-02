"""Tests for CollectsToScout."""

import os

from google3.testing.pybase import googletest

from google3.cityblock.special.legacy import collects_to_scout
from google3.cityblock.special.workflow.proto import scout_pb2
from google3.cityblock.special.workflow.tools import in_memory_workflow_service


class CollectsToScoutTests(googletest.TestCase):

  def testMerger(self):
    """Test to merge two lists of sites."""

    sites_1 = collects_to_scout.Parser('sample1.csv')
    sites_2 = collects_to_scout.Parser('sample2.csv')
    merged_sites = collects_to_scout.Merger(sites_1, sites_2)

    sites_1.extend(sites_2)
    expected_sites = sorted(sites_1,
                            key=lambda CurrentIssue: CurrentIssue.issue_name)

    self.assertEqual(expected_sites, merged_sites)

  def testToCSV(self):
    """Test to writing to CSV."""

    csv_file = 'sample.csv'
    sites = collects_to_scout.Parser(csv_file)
    collects_to_scout.ToCSV(csv_file, sites)

    expected_csv = 'sample_toScout.csv'
    actual_csv = None
    if os.path.exists(expected_csv):
      actual_csv = expected_csv

    self.assertEqual(expected_csv, actual_csv)

  def _AddSites(self, num_sites):
    """Adds sites to a datastore.

    Args:
      num_sites: number of sites to be added
    Returns:
      A tuple (workflow_service, site_ids_list).
    """

    workflow_service = in_memory_workflow_service.InMemoryWorkflowService()

    sites = collects_to_scout.Parser('sample.csv')
    count = 0
    site_ids = []
    for site in sites:
      count += 1
      if count <= num_sites:
        request = scout_pb2.CreateSiteRequest()
        request.site_metadata.name.translation = site.issue_name
        site.lat, site.lon = collects_to_scout.Geocode(site.address)
        request.site_metadata.lat = site.lat
        request.site_metadata.lng = site.lon
        response = workflow_service.CreateSite(request)
        site_ids.append(response.site_id)

    return workflow_service, site_ids

  def _GetSites(self, workflow_service, site_ids):
    """Gets all sites from a datastore.

    Args:
      workflow_service: The workflow service to read from.
      site_ids: Site ids of newly created sites
    Returns:
      An iterable of scout_pb2.SiteProto.
    """
    request = scout_pb2.GetSitesRequest(site_ids)
    response = workflow_service.GetSites(request)
    return response.site

  def testGetAllSites(self):
    """Tests getting all sites."""
    workflow_service, site_ids = self._AddSites(3)

    # Fetch all sites.
    sites = self._GetSites(workflow_service, site_ids)

    self.assertEqual(3, len(sites))

  def _AddCollections(self, num_collections):
    """Adds some collections to a datastore.

    Args:
      num_collections: The number of collections to add.
    Returns:
      A tuple (workflow_service, collection_ids_list).
    """
    workflow_service = in_memory_workflow_service.InMemoryWorkflowService()

    sites = collects_to_scout.Parser('sample.csv')
    count = 0
    collection_ids = []
    for site in sites:
      if count <= num_collections:
        # sites
        request = scout_pb2.CreateSiteRequest()
        request.site_metadata.name.translation = site.issue_name
        site.lat, site.lon = collects_to_scout.Geocode(site.address)
        request.site_metadata.lat = site.lat
        request.site_metadata.lng = site.lon
        response = workflow_service.CreateSite(request)
        site_id = response.site_id

	# Collections
        count += 1
        request = scout_pb2.CreateCollectionRequest()
        request.site_id = site_id
        request.collection_metadata.method = site.method.upper()
        response = workflow_service.CreateCollection(request)
        collection_ids.append(response.collection_id)

    return workflow_service, collection_ids

  def _GetCollections(self, workflow_service, collection_id):
    """Gets all collections from a datastore.

    Args:
      workflow_service: The workflow service to read from.
      collection_id: Collection id of a site.
    Returns:
      An iterable of scout_pb2.CollectionProto.
    """
    request = scout_pb2.GetCollectionsRequest()
    request.filter.collection_id = collection_id
    response = workflow_service.GetCollections(request.filter.collection_id)
    return response.collection

  def testGetAllCollections(self):
    """Tests getting all collections."""
    workflow_service, collection_ids = self._AddCollections(3)

    # Fetch all collections.
    collections = []
    for collection_id in collection_ids:
      collection = self._GetCollections(workflow_service, collection_id)
      collections.append(collection)

    self.assertEqual(3, len(collections))


if __name__ == '__main__':
  googletest.main()
