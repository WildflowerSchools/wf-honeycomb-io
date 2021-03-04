import honeycomb_io.core
import honeycomb_io.utils
import minimal_honeycomb
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Used by:
# honeycomb_io.uwb_data
def fetch_entity_info():
    logger.info(
        'Fetching entity assignment info to extract tray and person names')
    client = minimal_honeycomb.MinimalHoneycombClient()
    result = client.request(
        request_type="query",
        request_name='entityAssignments',
        arguments=None,
        return_object=[
            {'data': [
                'entity_assignment_id',
                {'entity': [
                    'entity_type: __typename',
                    {'... on Tray': [
                        'tray_id',
                        'tray_name: name'
                    ]},
                    {'... on Person': [
                        'person_id',
                        'person_name: name',
                        'person_short_name: short_name'
                    ]}
                ]}
            ]}
        ]
    )
    df = pd.json_normalize(result.get('data'))
    df.rename(
        columns={
            'entity.entity_type': 'entity_type',
            'entity.tray_id': 'tray_id',
            'entity.tray_name': 'tray_name',
            'entity.person_id': 'person_id',
            'entity.person_name': 'person_name',
            'entity.person_short_name': 'person_short_name',
        },
        inplace=True
    )
    df.set_index('entity_assignment_id', inplace=True)
    logger.info('Found {} entity assignments for trays and {} entity assignments for people'.format(
        df['tray_id'].notna().sum(),
        df['person_id'].notna().sum()
    ))
    return df

# Used by:
# camera_calibration.visualize (wf-camera-calibration)
def fetch_device_positions(
    environment_id,
    datetime,
    device_types,
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
    result=client.bulk_query(
        request_name='searchAssignments',
        arguments={
            'query': {
                'type': 'QueryExpression!',
                'value': {
                    'operator': 'AND',
                    'children': [
                        {
                            'field': 'environment',
                            'operator': 'EQ',
                            'value': environment_id
                        },
                        {
                            'field': 'assigned_type',
                            'operator': 'EQ',
                            'value': 'DEVICE'
                        }
                    ]
                }
            }
        },
        return_data=[
            'assignment_id',
            'start',
            'end',
            {'assigned': [
                {'... on Device': [
                    'device_id',
                    'name',
                    'device_type',
                    {'position_assignments': [
                        'start',
                        'end',
                        {'coordinate_space': [
                            'space_id'
                        ]},
                        'coordinates'
                    ]}
                ]}
            ]}
        ],
        id_field_name='assignment_id'
    )
    logger.info('Fetched {} device assignments'.format(len(result)))
    device_assignments = minimal_honeycomb.filter_assignments(
        result,
        start_time=datetime,
        end_time=datetime
    )
    logger.info('{} of these device assignments are active at specified datetime'.format(len(device_assignments)))
    device_assignments = list(filter(lambda x: x.get('assigned', {}).get('device_type') in device_types, result))
    logger.info('{} of these device assignments correspond to target device types'.format(len(device_assignments)))
    device_positions = dict()
    for device_assignment in device_assignments:
        device_id = device_assignment.get('assigned', {}).get('device_id')
        device_name = device_assignment.get('assigned', {}).get('name')
        if device_name is None:
            logger.info('Device {} has no name. Skipping.'.format(device_id))
            continue
        position_assignments = device_assignment.get('assigned', {}).get('position_assignments')
        if position_assignments is None:
            continue
        logger.info('Device {} has {} position assignments'.format(device_name, len(position_assignments)))
        position_assignments = minimal_honeycomb.filter_assignments(
            position_assignments,
            start_time=datetime,
            end_time=datetime
        )
        if len(position_assignments) > 1:
            raise ValueError('Device {} has multiple position assignments at specified datetime'.format(device_name))
        if len(position_assignments) == 0:
            logger.info('Device {} has no position assignments at specified datetime'.format(device_name))
            continue
        position_assignment = position_assignments[0]
        device_positions[device_id] = {
            'name': device_name,
            'position': position_assignment.get('coordinates')
        }
    return device_positions

def fetch_device_position(
    device_id,
    datetime,
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
    result=client.bulk_query(
        request_name='searchPositionAssignments',
        arguments={
            'query': {
                'type': 'QueryExpression!',
                'value': {
                    'operator': 'AND',
                    'children': [
                        {
                            'field': 'assigned',
                            'operator': 'EQ',
                            'value': device_id
                        },
                        {
                            'field': 'start',
                            'operator': 'LTE',
                            'value': honeycomb_io.utils.to_honeycomb_datetime(datetime)
                        },
                        {
                            'operator': 'OR',
                            'children': [
                                {
                                    'field': 'end',
                                    'operator': 'GTE',
                                    'value': honeycomb_io.utils.to_honeycomb_datetime(datetime)
                                },
                                {
                                    'field': 'end',
                                    'operator': 'ISNULL'
                                }
                            ]
                        }
                    ]
                }
            }
        },
        return_data=[
            'position_assignment_id',
            'coordinates'
        ],
        id_field_name='position_assignment_id'
    )
    if len(result) == 0:
        return None
    if len(result) > 1:
        raise ValueError('More than one position assignment consistent with specified device ID and time')
    device_position = result[0].get('coordinates')
    return device_position

# Used by:
# camera_calibration.colmap (wf-camera-calibration)
def write_position_data(
    data,
    start_datetime,
    coordinate_space_id,
    assigned_type='DEVICE',
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    position_data_columns = [
        'device_id',
        'position'
    ]
    if not set(position_data_columns).issubset(set(data.columns)):
        raise ValueError('Data must contain the following columns: {}'.format(
            position_data_columns
        ))
    position_data_df = data.reset_index().reindex(columns=position_data_columns)
    position_data_df.rename(columns={'device_id': 'assigned'}, inplace=True)
    position_data_df.rename(columns={'position': 'coordinates'}, inplace=True)
    position_data_df['start'] = honeycomb_io.utils.to_honeycomb_datetime(start_datetime)
    position_data_df['assigned_type'] = assigned_type
    position_data_df['coordinate_space'] = coordinate_space_id
    position_data_df['coordinates'] = position_data_df['coordinates'].apply(lambda x: x.tolist())
    records = position_data_df.to_dict(orient='records')
    if client is None:
        client = minimal_honeycomb.MinimalHoneycombClient(
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    result=client.bulk_mutation(
        request_name='assignToPosition',
        arguments={
            'positionAssignment': {
                'type': 'PositionAssignmentInput!',
                'value': records
            }
        },
        return_object=[
            'position_assignment_id'
        ]
    )
    ids = None
    if len(result) > 0:
        ids = [datum.get('position_assignment_id') for datum in result]
    return ids
