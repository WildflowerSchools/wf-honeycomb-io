import honeycomb_io.schema
import honeycomb_io.utils
import minimal_honeycomb
import inflection
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

def search_objects(
    request_name,
    query_list,
    return_data,
    id_field_name,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
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
        chunk_size=chunk_size
    )
    return result

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
