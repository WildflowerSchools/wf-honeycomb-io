import minimal_honeycomb
import logging

logger = logging.getLogger(__name__)

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
