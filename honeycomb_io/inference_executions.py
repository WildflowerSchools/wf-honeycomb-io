import honeycomb_io.core
import honeycomb_io.utils
import minimal_honeycomb
import pandas as pd
import numpy as np
import datetime
import logging

logger = logging.getLogger(__name__)

# Used by:
# honeycomb_io.poses
def fetch_inference_ids(
    inference_ids=None,
    inference_names=None,
    inference_models=None,
    inference_versions=None,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if inference_ids is not None:
        if inference_names is not None or inference_models is not None or inference_versions is not None:
            raise ValueError('If inference IDs are specified, inference names/models/versions cannot be specified')
        return inference_ids
    if inference_names is not None or inference_models is not None or inference_versions is not None:
        query_list=list()
        if inference_names is not None:
            query_list.append({
                'field': 'name',
                'operator': 'IN',
                'values': inference_names
            })
        if inference_models is not None:
            query_list.append({
                'field': 'model',
                'operator': 'IN',
                'values': inference_models
            })
        if inference_versions is not None:
            query_list.append({
                'field': 'version',
                'operator': 'IN',
                'values': inference_versions
            })
        logger.info('Fetching inference IDs for inference runs with specified properties')
        client = honeycomb_io.core.generate_client(
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
        result = client.bulk_query(
            request_name='searchInferenceExecutions',
            arguments={
                'query': {
                    'type': 'QueryExpression!',
                    'value': {
                        'operator': 'AND',
                        'children': query_list
                    }
                }
            },
            return_data=[
                'inference_id'
            ],
            id_field_name='inference_id'
        )
        if len(result) == 0:
            raise ValueError('No inference executions match specified inference names/models/versions')
        inference_ids = [datum.get('inference_id') for datum in result]
        logger.info('Found {} inference runs that match specified properties'.format(len(inference_ids)))
        return inference_ids
    return None

# Not currently used
def create_inference_execution(
    execution_start=None,
    name=None,
    notes=None,
    model=None,
    version=None,
    data_sources=None,
    data_results=None,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if execution_start is None:
        execution_start = honeycomb_io.utils.to_honeycomb_datetime(datetime.datetime.now(tz=datetime.timezone.utc))
    else:
        execution_start = honeycomb_io.utils.to_honeycomb_datetime(execution_start)
    client = honeycomb_io.core.generate_client(
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    logger.info('Creating inference execution')
    result = client.request(
        request_type='mutation',
        request_name='createInferenceExecution',
        arguments={
            'inferenceExecution': {
                'type': 'InferenceExecutionInput',
                'value': {
                    'execution_start': execution_start,
                    'name': name,
                    'notes': notes,
                    'model': model,
                    'version': version,
                    'data_sources': data_sources,
                    'data_results': data_results
                }
            }
        },
        return_object=[
            'inference_id'
        ]
    )
    try:
        inference_id = result['inference_id']
    except:
        raise ValueError('Received unexpected response from Honeycomb: {}'.format(result))
    return inference_id

# Not currently used
def delete_inference_execution(
    inference_id,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    client = honeycomb_io.core.generate_client(
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    result = client.request(
        request_type='mutation',
        request_name='deleteInferenceExecution',
        arguments={
            'inference_id': {
                'type': 'ID',
                'value': inference_id
            }
        },
        return_object=['status']
    )
    status = result.get('status')
    return status
