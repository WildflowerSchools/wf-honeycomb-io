import honeycomb_io.schema
import honeycomb_io.utils
import minimal_honeycomb
import inflection
import datetime
import logging

logger = logging.getLogger(__name__)

def create_objects(
    object_name=None,
    data=None,
    request_name=None,
    argument_name=None,
    argument_type=None,
    id_field_name=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if data is None:
        logger.warn('No data supplied')
        ids = list()
        return ids
    if request_name is None:
        if object_name is None:
            raise ValueError('Must specify either request name or object name')
        request_name = honeycomb_io.schema.create_endpoint_name(object_name=object_name)
    if argument_name is None:
        if object_name is None:
            raise ValueError('Must specify either argument name or object name')
        argument_name = honeycomb_io.schema.create_endpoint_argument_name(object_name=object_name)
    if argument_type is None:
        if object_name is None:
            raise ValueError('Must specify either argument type or object name')
        argument_type = honeycomb_io.schema.create_endpoint_argument_type(object_name=object_name)
    if id_field_name is None:
        if object_name is None:
            raise ValueError('Must specify either ID field name or object name')
        id_field_name = honeycomb_io.schema.id_field_name(object_name=object_name)
    data_list = honeycomb_io.utils.parse_data_sequence(data=data)
    client = generate_client(
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    result = client.bulk_mutation(
        request_name=request_name,
        arguments = {
            argument_name: {
                'type': argument_type,
                'value': data_list
            }
        },
        return_object = [
            id_field_name
        ],
        chunk_size=chunk_size
    )
    if not isinstance(result, list):
        raise ValueError('Received unexpected result from Honyecomb: {}'.format(
            result
        ))
    ids = [datum.get(id_field_name) for datum in result]
    return ids

def update_objects(
    object_name=None,
    data=None,
    request_name=None,
    argument_name=None,
    argument_type=None,
    id_field_name=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if data is None:
        logger.warn('No data supplied')
        updated_data = list()
        return updated_data
    if request_name is None:
        if object_name is None:
            raise ValueError('Must specify either request name or object name')
        request_name = honeycomb_io.schema.update_endpoint_name(object_name=object_name)
    if argument_name is None:
        if object_name is None:
            raise ValueError('Must specify either argument name or object name')
        argument_name = honeycomb_io.schema.update_endpoint_argument_name(object_name=object_name)
    if argument_type is None:
        if object_name is None:
            raise ValueError('Must specify either argument type or object name')
        argument_type = honeycomb_io.schema.update_endpoint_argument_type(object_name=object_name)
    if id_field_name is None:
        if object_name is None:
            raise ValueError('Must specify either ID field name or object name')
        id_field_name = honeycomb_io.schema.id_field_name(object_name=object_name)
    data_list = honeycomb_io.utils.parse_data_sequence(data=data)
    ids = list()
    data_fields = set()
    for datum in data_list:
        data_fields = data_fields.union(datum.keys())
        if id_field_name not in datum.keys():
            raise ValueError('Every update data object must contain ID field \'{}\''.format(
                id_field_name
            ))
        ids.append(datum.pop(id_field_name))
    data_fields = list(data_fields)
    client = generate_client(
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    result = client.bulk_mutation(
        request_name=request_name,
        arguments = {
            id_field_name: {
                'type': 'ID!',
                'value': ids
            },
            argument_name: {
                'type': argument_type,
                'value': data_list
            }
        },
        return_object=data_fields,
        chunk_size=chunk_size
    )
    if not isinstance(result, list):
        raise ValueError('Received unexpected result from Honyecomb: {}'.format(
            result
        ))
    updated_data = result
    return updated_data

def search_objects(
    object_name=None,
    query_list=None,
    return_data=None,
    request_name=None,
    id_field_name=None,
    sort_arguments=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if query_list is None:
        logger.warn('No query specified')
        data = list()
        return data
    if return_data is None:
        logger.warn('No return data specified')
        data = list()
        return data
    if request_name is None:
        if object_name is None:
            raise ValueError('Must specify either request name or object name')
        request_name = honeycomb_io.schema.search_endpoint_name(object_name=object_name)
    if id_field_name is None:
        if object_name is None:
            raise ValueError('Must specify either ID field name or object name')
        id_field_name = honeycomb_io.schema.id_field_name(object_name=object_name)
    client = generate_client(
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    result = client.bulk_query(
        request_name=request_name,
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
        id_field_name=id_field_name,
        chunk_size=chunk_size,
        sort_arguments=sort_arguments
    )
    if not isinstance(result, list):
        raise ValueError('Received unexpected result from Honyecomb: {}'.format(
            result
        ))
    return result

def fetch_latest_object(
    object_name=None,
    query_list=None,
    return_data=None,
    request_name=None,
    id_field_name=None,
    timestamp_field='timestamp',
    time_limit_hours=24,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if query_list is None:
        logger.warn('No query specified')
        data = list()
        return data
    if return_data is None:
        logger.warn('No return data specified')
        data = list()
        return data
    if request_name is None:
        if object_name is None:
            raise ValueError('Must specify either request name or object name')
        request_name = honeycomb_io.schema.search_endpoint_name(object_name=object_name)
    if id_field_name is None:
        if object_name is None:
            raise ValueError('Must specify either ID field name or object name')
        id_field_name = honeycomb_io.schema.id_field_name(object_name=object_name)
    if timestamp_field not in return_data:
        raise ValueError('Timestamp field \'{}\' must be included in return data specification'.format(
            timestamp_field
        ))
    time_limit = honeycomb_io.utils.to_honeycomb_datetime(
        datetime.datetime.now(tz=datetime.timezone.utc) -
        datetime.timedelta(hours=time_limit_hours)
    )
    try:
        query_list.append({'field': timestamp_field, 'operator': 'GTE', 'value': time_limit})
    except:
        query_list=[{'field': timestamp_field, 'operator': 'GTE', 'value': time_limit}]
    client = generate_client(
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    result = client.request(
        request_type='query',
        request_name=request_name,
        arguments={
            'query': {
                'type': 'QueryExpression!',
                'value': {
                    'operator': 'AND',
                    'children': query_list
                }
            },
            'page': {
                'type': 'PaginationInput',
                'value': {
                    'max': 1,
                    'sort': {
                        'field': timestamp_field,
                        'direction': 'DESC'
                    }
                }
            }
        },
        return_object = [
            {'data': return_data}
        ]
    )
    if not isinstance(result, dict) or not isinstance(result.get('data'), list):
        raise ValueError('Received unexpected result from Honyecomb: {}'.format(
            result
        ))
    data = result.get('data')
    if len(data) == 0:
        logger.warning('No objects returned for endpoint {} and search query list {}'.format(
            request_name,
            query_list
        ))
        return None
    return data[0]

def delete_objects(
    object_name=None,
    ids=None,
    request_name=None,
    id_field_name=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if ids is None:
        logger.warn('No IDs specified')
        status = None
        return status
    if request_name is None:
        if object_name is None:
            raise ValueError('Must specify either request name or object name')
        request_name = honeycomb_io.schema.delete_endpoint_name(object_name=object_name)
    if id_field_name is None:
        if object_name is None:
            raise ValueError('Must specify either ID field name or object name')
        id_field_name = honeycomb_io.schema.id_field_name(object_name=object_name)
    data_id_list = honeycomb_io.utils.parse_data_id_sequence(ids=ids)
    client = generate_client(
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    result = client.bulk_mutation(
        request_name=request_name,
        arguments = {
            id_field_name: {
                'type': 'ID',
                'value': data_id_list
            }
        },
        return_object = [
            'status',
            'error'
        ],
        chunk_size=chunk_size
    )
    if not isinstance(result, list):
        raise ValueError('Received unexpected result from Honyecomb: {}'.format(
            result
        ))
    status = result
    return result

def generate_client(
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if client is None:
        client=minimal_honeycomb.MinimalHoneycombClient(
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    return client
