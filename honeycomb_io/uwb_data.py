import honeycomb_io.core
import minimal_honeycomb
import pandas as pd
import numpy as np
import json
import logging

logger = logging.getLogger(__name__)


def fetch_uwb_data_data_id(
    data_id,
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
    result=client.request(
        request_type='query',
        request_name='getDatapoint',
        arguments={
            'data_id': {
                'type': 'ID!',
                'value': data_id
            }
        },
        return_object = [
            'timestamp',
            {'source': [
                {'... on Assignment': [
                    'assignment_id'
                ]}
            ]},
            {'file': [
                'data'
            ]}
        ]
    )
    datapoint_timestamp=minimal_honeycomb.from_honeycomb_datetime(result.get('timestamp'))
    assignment_id=result.get('source', {}).get('assignment_id')
    data_jsonl_json = result.get('file', {}).get('data')
    if data_jsonl_json is None:
        logger.warn('No UWB data returned')
        return pd.DataFrame()
    try:
        data_jsonl = json.loads(data_jsonl_json)
    except:
        raise ValueError('Expected JSONL wrapped as JSON, but JSON deserialization failed')
    if not isinstance(data_jsonl, str):
        raise ValueError('Expected JSONL but got type \'{}\''.format(type(data_jsonl)))
    data_dict_list = list()
    for data_jsonl_line in data_jsonl.split('\n'):
        try:
            data_dict_list.append(json.loads(data_jsonl_line))
        except:
            logger.warn('Encountered malformed JSONL line. Omitting.')
            continue
    df = pd.DataFrame(data_dict_list)
    original_columns = df.columns.tolist()
    df['assignment_id'] = assignment_id
    new_columns = ['assignment_id'] + original_columns
    df = df.reindex(columns=new_columns)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    if df['timestamp'].isna().any():
        logger.warn('Returned UWB data is missing some timestamp data')
    df = df.dropna(subset=['timestamp']).reset_index(drop=True)
    logger.info('Datapoint {} with timestamp {} yielded {} observations from {} to {}. Serial numbers: {}. Types: {}'.format(
        data_id,
        datapoint_timestamp.isoformat(),
        len(df),
        df['timestamp'].min().isoformat(),
        df['timestamp'].max().isoformat(),
        df['serial_number'].value_counts().to_dict(),
        df['type'].value_counts().to_dict()
    ))
    return df

def extract_position_data(
    df
):
    if len(df) == 0:
        return df
    df = df.loc[df['type'] == 'position'].copy().reset_index(drop=True)
    if len(df) != 0:
        df['x_position'] = df['x'] / 1000.0
        df['y_position'] = df['y'] / 1000.0
        df['z_position'] = df['z'] / 1000.0
        df['anchor_count'] = pd.to_numeric(df['anchor_count']).astype('Int64')
        df['quality'] = pd.to_numeric(df['quality']).astype('Int64')
    df = df.reindex(columns=[
        'assignment_id',
        'timestamp',
        'object_id',
        'serial_number',
        'x_position',
        'y_position',
        'z_position',
        'anchor_count',
        'quality'
    ])
    return df

def fetch_uwb_data_ids(
    datapoint_timestamp_min,
    datapoint_timestamp_max,
    assignment_ids,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    query_list = [
        {'field': 'timestamp', 'operator': 'GTE', 'value': datapoint_timestamp_min},
        {'field': 'timestamp', 'operator': 'LTE', 'value': datapoint_timestamp_max},
        {'field': 'source', 'operator': 'IN', 'values': assignment_ids}
    ]
    return_data = [
        'data_id'
    ]
    id_field_name='data_id'
    result = honeycomb_io.core.search_objects(
        request_name='searchDatapoints',
        query_list=query_list,
        return_data=return_data,
        id_field_name=id_field_name,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    data_ids = [datum.get('data_id') for datum in result]
    return data_ids

def fetch_person_tag_info(
    start,
    end,
    environment_id,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    query_list = [
        {'field': 'environment', 'operator': 'EQ', 'value': environment_id},
        {'field': 'start', 'operator': 'LTE', 'value': end},
        {'operator': 'OR', 'children': [
            {'field': 'end', 'operator': 'ISNULL'},
            {'field': 'end', 'operator': 'GTE', 'value': start},
        ]},
        {'field': 'assigned_type', 'operator': 'EQ', 'value': 'DEVICE'}
    ]
    return_data = [
        'assignment_id',
        'start',
        'end',
        {'assigned': [
            {'... on Device': [
                'device_id',
                'device_type',
                'name',
                'tag_id',
                {'entity_assignments': [
                    'start',
                    'end',
                    'entity_type',
                    {'entity': [
                        {'...on Person': [
                            'person_id',
                            'person_type',
                            'name',
                            'short_name'
                        ]}
                    ]}
                ]}
            ]}
        ]}
    ]
    id_field_name='assignment_id'
    result = honeycomb_io.core.search_objects(
        request_name='searchAssignments',
        query_list=query_list,
        return_data=return_data,
        id_field_name=id_field_name,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    result = list(filter(
        lambda assignment: assignment.get('assigned', {}).get('device_type') == 'UWBTAG',
        result
    ))
    for assignment in result:
        assignment['assigned']['entity_assignments'] = minimal_honeycomb.filter_assignments(
            assignments=assignment['assigned']['entity_assignments'],
            start_time=start,
            end_time=end
        )
        if len(assignment['assigned']['entity_assignments']) > 1:
            raise ValueError('UWB tag {} has multiple entity assignments in the specified period ({} to {})'.format(
                assignment.get('assigned', {}).get('name'),
                start.isoformat(),
                end.isoformat()
            ))
    result = list(filter(
        lambda assignment: len(assignment.get('assigned', {}).get('entity_assignments')) == 1,
        result
    ))
    result = list(filter(
        lambda assignment: assignment.get('assigned', {}).get('entity_assignments')[0].get('entity_type') == 'PERSON',
        result
    ))
    data_list=list()
    for assignment in result:
        data_list.append({
            'assignment_id': assignment.get('assignment_id'),
            'device_id': assignment.get('assigned', {}).get('device_id'),
            'device_name': assignment.get('assigned', {}).get('name'),
            'tag_id': assignment.get('assigned', {}).get('tag_id'),
            'person_id': assignment.get('assigned', {}).get('entity_assignments')[0].get('entity', {}).get('person_id'),
            'person_type': assignment.get('assigned', {}).get('entity_assignments')[0].get('entity', {}).get('person_type'),
            'person_name': assignment.get('assigned', {}).get('entity_assignments')[0].get('entity', {}).get('name'),
            'short_name': assignment.get('assigned', {}).get('entity_assignments')[0].get('entity', {}).get('short_name')
        })
    df = pd.DataFrame(data_list)
    df.set_index('assignment_id', inplace=True)
    return df

def add_person_tag_info(
    uwb_data_df,
    person_tag_info_df
):
    uwb_data_df = uwb_data_df.join(person_tag_info_df, on='assignment_id')
    return uwb_data_df
