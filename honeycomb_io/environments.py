import honeycomb_io.core
import honeycomb_io.utils
import minimal_honeycomb
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Used by:
# honeycomb_io.cameras
# video_io.core (wf-video-io)
# camera_calibration.colmap (wf-video-io)
def fetch_environment_id(
    environment_id=None,
    environment_name=None,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if environment_id is not None:
        if environment_name is not None:
            raise ValueError('If environment ID is specified, environment name cannot be specified')
        return environment_id
    if environment_name is not None:
        logger.info('Fetching environment ID for specified environment name')
        client = honeycomb_io.core.generate_client(
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
        result = client.bulk_query(
            request_name='findEnvironments',
            arguments={
                'name': {
                    'type': 'String',
                    'value': environment_name
                }
            },
            return_data=[
                'environment_id'
            ],
            id_field_name='environment_id'
        )
        if len(result) == 0:
            raise ValueError('No environments match environment name {}'.format(
                environment_name
            ))
        if len(result) > 1:
            raise ValueError('Multiple environments match environment name {}'.format(
                environment_name
            ))
        environment_id = result[0].get('environment_id')
        logger.info('Found environment ID for specified environment name')
        return environment_id
    return None

# Used by:
# process_cuwb_data.core (wf-process-cuwb-data)
def fetch_environment_by_name(environment_name):
    logger.info('Fetching Environments data')
    client = minimal_honeycomb.MinimalHoneycombClient()
    result = client.request(
        request_type="query",
        request_name="environments",
        arguments=None,
        return_object=[
            {'data':
                [
                    'environment_id',
                    'name'
                ]
             }
        ]
    )
    logger.info('Found environments data: {} records'.format(
        len(result.get('data'))))
    df = pd.DataFrame(result.get('data'))
    df = df[df['name'].str.lower().isin([environment_name.lower()])].reset_index(drop=True)
    if len(df) > 0:
        return df.loc[0]
    return None

# Used by:
# honeycomb_io.uwb_data
def fetch_device_assignments(
    start,
    end,
    environment_id=None,
    environment_name=None,
    device_type=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    start = pd.to_datetime(start, utc=True).to_pydatetime()
    end = pd.to_datetime(end, utc=True).to_pydatetime()
    environment_id = fetch_environment_id(
        environment_id=environment_id,
        environment_name=environment_name,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    query_list = list()
    query_list.append({
        'field': 'environment',
        'operator': 'EQ',
        'value': environment_id
    })
    query_list.append({
        'field': 'start',
        'operator': 'LTE',
        'value': honeycomb_io.utils.to_honeycomb_datetime(start)
    })
    query_list.append({
        'operator': 'OR',
        'children': [
            {'field': 'end', 'operator': 'ISNULL'},
            {'field': 'end', 'operator': 'GTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(end)}
        ]
    })
    query_list.append({
        'field': 'assigned_type',
        'operator': 'EQ',
        'value': 'DEVICE'
    })
    assignments = honeycomb_io.core.search_objects(
        object_name='Assignment',
        query_list=query_list,
        return_data = [
            'assignment_id',
            'start',
            'end',
            {'assigned': [
                {'... on Device': [
                    'device_id',
                    'part_number',
                    'device_type',
                    'name',
                    'tag_id',
                    'serial_number',
                    'mac_address',
                    'description'
                ]}
            ]}

        ],
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    if len(assignments) == 0:
        logger.warn('No assignments found')
        return pd.DataFrame()
    dict_list = list()
    for assignment in assignments:
        dict_item = assignment['assigned']
        dict_item['assignment_id'] = assignment['assignment_id']
        dict_item['start'] = assignment['start']
        dict_item['end'] = assignment['end']
        dict_list.append(dict_item)
    df = pd.DataFrame(dict_list)
    df['start'] = pd.to_datetime(df['start'])
    df['end'] = pd.to_datetime(df['end'])
    df = df.reindex(columns=[
        'assignment_id',
        'device_id',
        'part_number',
        'device_type',
        'name',
        'tag_id',
        'serial_number',
        'mac_address',
        'description',
        'start',
        'end'
    ])
    df.set_index('assignment_id', inplace=True)
    if device_type is not None:
        df = df.loc[df['device_type'] == device_type].copy()
    return df

def filter_assignments(
    assignments,
    environment_id=None,
    environment_name=None,
    start=None,
    end=None
):
    filtered_assignments = list(filter(
        lambda assignment: (
            (
                environment_id is None or
                assignment.get('environment').get('environment_id') == environment_id
            ) and
            (
                environment_name is None or
                assignment.get('environment').get('name') == environment_name
            ) and
            (
                start is None or
                assignment.get('end') is None or
                pd.to_datetime(assignment.get('end'), utc=True) >= pd.to_datetime(start, utc=True)
            ) and
            (
                end is None or
                pd.to_datetime(assignment.get('start'), utc=True) <= pd.to_datetime(end, utc=True)
            )
        ),
        assignments
    ))
    return filtered_assignments
