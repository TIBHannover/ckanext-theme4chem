import ckan.lib.helpers as h
import ckan.plugins.toolkit as toolkit
import ckan.plugins as plugins
import datetime
from ckan import model
from sqlalchemy import func

from collections import Counter
import logging
log = logging.getLogger(__name__)


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

def org_last_harvest_time(org_id_or_name):
    """
    Return the last finished harvest time (as ISO string or None)
    for all harvest sources belonging to a given organization.
    """
    context = {"ignore_auth": True}

    # 1) Get harvest sources for this org
    search = toolkit.get_action("package_search")(
        context,
        {
            "fq": "dataset_type:harvest owner_org:{0}".format(org_id_or_name),
            "rows": 1000,  # adjust if you have more than 1000 sources
        },
    )

    last_ts = None

    for pkg in search["results"]:
        # HarvestSource id == Package id, so we can pass pkg["id"] here
        src = toolkit.get_action("harvest_source_show")(
            context,
            {"id": pkg["id"], "include_status": True}
        )

        status = src.get("status") or {}
        last_job = status.get("last_job") or {}

        # Prefer finished time, fall back to created if needed
        finished = (
            last_job.get("finished")
            or last_job.get("gather_finished")
            or last_job.get("created")
        )

        if finished and (last_ts is None or finished > last_ts):
            last_ts = finished
    return last_ts