import minimal_honeycomb
import logging

logger = logging.getLogger(__name__)

# Used by:
# inference_helpers.jobs.prepare (wf-inference-helpers)
def get_datapoint_keys_for_assignment_in_range(assignment_id, start, end, honeycomb_client=None):
    if honeycomb_client is not None:
        raise ValueError('Option of specifying a Honeycomb client has been removed')
    if honeycomb_client is None:
        honeycomb_client = honeycomb_io.core.get_legacy_client()
    query_pages = """
        query searchDatapoints($cursor: String, $assignment_id: String, $start: String, $end: String) {
          searchDatapoints(
            query: { operator: AND, children: [
                { operator: EQ, field: "source", value: $assignment_id },
                { operator: GTE, field: "timestamp", value: $start },
                { operator: LT, field: "timestamp", value: $end },
            ] }
            page: { cursor: $cursor, max: 1000, sort: {field: "timestamp", direction: DESC} }
          ) {
            page_info {
              count
              cursor
            }
            data {
              data_id
              timestamp
              file {
                key
                bucketName
              }
            }
          }
        }
        """
    cursor = ""
    while True:
        vari = {"assignment_id": assignment_id, "start": start, "end": end, "cursor": cursor}
        page = honeycomb_client.raw_query(query_pages, vari)
        page_info = page.get("searchDatapoints").get("page_info")
        data = page.get("searchDatapoints").get("data")
        cursor = page_info.get("cursor")
        if page_info.get("count") == 0:
            break
        for item in data:
            yield item

# Used by:
# video_io.core (wf-video-io)
def search_datapoints(
    query_list,
    return_data,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Searching for datapoints that match the specified parameters')
    if client is None:
        client = minimal_honeycomb.MinimalHoneycombClient(
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    result = client.bulk_query(
        request_name='searchDatapoints',
        arguments={
            'query': {
                'type': 'QueryExpression!',
                'value': {
                    'operator': 'AND',
                    'children': query_list
                }
            }
        },
        return_data=return_data,
        id_field_name = 'data_id',
        chunk_size=chunk_size
    )
    logger.info('Fetched {} datapoints'.format(len(result)))
    return result
