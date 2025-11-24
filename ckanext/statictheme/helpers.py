import ckan.lib.helpers as h
import ckan.plugins.toolkit as toolkit
import ckan.plugins as plugins
import datetime
from ckan import model
from sqlalchemy import func
from ckan.model.meta import Session
from ckan.model.package import Package
from ckanext.harvest.model import HarvestSource, HarvestJob  # <-- THIS LINE
from collections import Counter
import logging
log = logging.getLogger(__name__)

# --- simple in-memory cache ---
_LAST_HARVEST_CACHE = {}  # { org_key: {"time": <timestamp>, "value": <iso_or_None>} }
_CACHE_TTL = 300          # seconds (5 minutes). Adjust as you like.

# def repositories_dataset_present_count():
#     """Number of repositories in CKAN organizations &
#     Number of datasets in the repositories list. """
#     each_repo_count = []
#     organization_details = []
#
#     list_org = toolkit.get_action('organization_list')(
#         data_dict={'type': 'repository', 'sort': 'package_count desc', 'all_fields': True})
#
#     count_to_display_repo = len(list_org)
#
#     for org in list_org:
#         try:
#             get_org = toolkit.get_action('organization_show')(
#                 data_dict={
#                     'id': org['id'],
#                     'include_datasets': True,
#                     'include_dataset_count': True
#                 }
#             )
#
#             # Filter only datasets of type 'dataset'
#             filtered_datasets = [
#                 ds for ds in get_org.get('packages', [])
#                 if ds.get('type') == 'dataset'
#             ]
#
#             # Sort by metadata_modified descending and take 5 latest
#             latest_datasets = sorted(
#                 filtered_datasets,
#                 key=lambda x: x.get('metadata_modified', ''),
#                 reverse=True
#             )[:5]
#
#             # Overwrite 'packages' key with the reduced list
#             get_org['packages'] = latest_datasets
#
#             organization_details.append(get_org)
#
#         except toolkit.ObjectNotFound:
#             log.error(f"Organization '{org}' not found.")
#
#     for package_count in list_org:
#         each_repo_count.append(package_count['package_count'])
#
#     count_to_display_dataset = sum(each_repo_count)
#
#
#     return count_to_display_repo, count_to_display_dataset, organization_details,


def repositories_dataset_present_count():
    """
    Number of repositories in CKAN organizations &
    Number of datasets in the repositories list.
    Also returns organization details and a mapping of datasets to metadata_modified.
    """
    each_repo_count = []
    organization_details = []

    # NEW: mappings of dataset -> metadata_modified
    datasets_modified = {}              # flat map: {dataset_id: metadata_modified}
    datasets_modified_by_org = {}       # nested: {org_id: {dataset_id: metadata_modified}}

    list_org = toolkit.get_action('organization_list')(
        data_dict={'type': 'repository', 'sort': 'package_count desc', 'all_fields': True}
    )

    count_to_display_repo = len(list_org)

    for org in list_org:
        try:
            get_org = toolkit.get_action('organization_show')(
                data_dict={
                    'id': org['id'],
                    'include_datasets': True,
                    'include_dataset_count': True
                }
            )

            # Filter only datasets of type 'dataset'
            filtered_datasets = [
                ds for ds in get_org.get('packages', [])
                if ds.get('type') == 'dataset'
            ]

            # Sort by metadata_modified descending and take 5 latest
            latest_datasets = sorted(
                filtered_datasets,
                key=lambda x: x.get('metadata_modified', ''),
                reverse=True
            )[:5]

            # Overwrite 'packages' key with the reduced list
            get_org['packages'] = latest_datasets
            organization_details.append(get_org)

            # Build the mappings
            datasets_modified_by_org[get_org['id']] = {
                ds['id']: ds.get('metadata_modified') for ds in latest_datasets
            }
            for ds in latest_datasets:
                datasets_modified[ds['id']] = ds.get('metadata_modified')

        except toolkit.ObjectNotFound:
            log.error(f"Organization '{org}' not found.")

    for package_count in list_org:
        each_repo_count.append(package_count.get('package_count', 0))

    count_to_display_dataset = sum(each_repo_count)

    return (
        count_to_display_repo,
        count_to_display_dataset,
        organization_details,
        datasets_modified,          # flat
        datasets_modified_by_org    # grouped
    )


def get_measurement_count(name,search_facets):
    """Number of datasets with measurement_technique_proxy field present."""

    return 'Nothing o Return'


def get_recent_datasets_by_org():

    one_month_ago = datetime.datetime.utcnow() - datetime.timedelta(days=90)

    query = (
        model.Session.query(model.Package.owner_org, func.count(model.Package.id))
        .filter(model.Package.metadata_created >= one_month_ago)
        .filter(model.Package.state == 'active')
        .filter(model.Package.private == False)
        .group_by(model.Package.owner_org)
    )

    result = {org_id: count for org_id, count in query}

    return result

def _compute_org_last_harvest_time(org_id_or_name):

    """
    Return the last harvest time (ISO string or None) for all
    harvest sources belonging to the given organization.

    Uses a single SQL query:
      HarvestJob -> HarvestSource -> Package (type='harvest')
    where Package.owner_org == org_id and Package.id == HarvestSource.id.
    """
    # Resolve org id, in case a name is passed
    context = {"ignore_auth": True}

    # Resolve org id (works with id or name)
    try:
        org = toolkit.get_action("organization_show")(context, {"id": org_id_or_name})
    except toolkit.ObjectNotFound:
        log.warning("Organization %r not found", org_id_or_name)
        return None

    org_id = org["id"]

    # MAX( COALESCE(finished, gather_finished, created) )
    # for all jobs of harvest sources whose package belongs to this org
    last_dt = (
        Session.query(
            func.max(
                func.coalesce(
                    HarvestJob.finished,
                    HarvestJob.gather_finished,
                    HarvestJob.created,
                )
            )
        )
        .join(HarvestSource, HarvestJob.source_id == HarvestSource.id)
        .join(Package, Package.id == HarvestSource.id)
        .filter(Package.type == "harvest")
        .filter(Package.owner_org == org_id)
        .scalar()
    )

    if not last_dt:
        return None

    log.debug(f"{last_dt}")

    return last_dt.replace(microsecond=0).strftime("%H:%M %d-%m-%Y")


def org_last_harvest_time(org_id_or_name):
    """
    Cached wrapper around _compute_org_last_harvest_time.

    Returns ISO string or None.
    """
    now = datetime.datetime.utcnow()
    key = org_id_or_name

    cached = _LAST_HARVEST_CACHE.get(key)
    if cached:
        age = (now - cached["time"]).total_seconds()  # convert timedelta → seconds
        if age < _CACHE_TTL:
            return cached["value"]

    value = _compute_org_last_harvest_time(org_id_or_name)

    if value is not None and hasattr(value, "isoformat"):
        value = value.isoformat()

    _LAST_HARVEST_CACHE[key] = {"time": now, "value": value}
    return value
