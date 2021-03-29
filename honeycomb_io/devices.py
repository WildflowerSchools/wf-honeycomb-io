import honeycomb_io.core
import honeycomb_io.utils
import minimal_honeycomb
import pandas as pd
from collections import Counter
import logging

logger = logging.getLogger(__name__)

def fetch_device_ids(
    device_types=None,
    device_ids=None,
    part_numbers=None,
    serial_numbers=None,
    tag_ids=None,
    names=None,
    environment_id=None,
    environment_name=None,
    start=None,
    end=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    devices = fetch_devices(
        device_types=device_types,
        device_ids=device_ids,
        part_numbers=part_numbers,
        serial_numbers=serial_numbers,
        tag_ids=tag_ids,
        names=names,
        environment_id=environment_id,
        environment_name=environment_name,
        start=start,
        end=end,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    device_ids = [device['device_id'] for device in devices]
    return device_ids

def fetch_devices(
    device_types=None,
    device_ids=None,
    part_numbers=None,
    serial_numbers=None,
    tag_ids=None,
    names=None,
    environment_id=None,
    environment_name=None,
    start=None,
    end=None,
    output_format='list',
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if (
        device_types is not None or
        device_ids is not None or
        part_numbers is not None or
        serial_numbers is not None or
        tag_ids is not None or
        names is not None
    ):
        query_list = list()
        if device_types is not None:
            query_list.append(
                {'field': 'device_type', 'operator': 'CONTAINED_BY', 'values': device_types}
            )
        if device_ids is not None:
            query_list.append(
                {'field': 'device_id', 'operator': 'CONTAINED_BY', 'values': device_ids}
            )
        if part_numbers is not None:
            query_list.append(
                {'field': 'part_number', 'operator': 'CONTAINED_BY', 'values': part_numbers}
            )
        if serial_numbers is not None:
            query_list.append(
                {'field': 'serial_number', 'operator': 'CONTAINED_BY', 'values': serial_numbers}
            )
        if tag_ids is not None:
            query_list.append(
                {'field': 'tag_id', 'operator': 'CONTAINED_BY', 'values': tag_ids}
            )
        if names is not None:
            query_list.append(
                {'field': 'name', 'operator': 'CONTAINED_BY', 'values': names}
            )
        return_data = [
            'device_id',
            'device_type',
            'part_number',
            'serial_number',
            'tag_id',
            'name',
            {'assignments': [
                'assignment_id',
                'start',
                'end',
                {'environment': [
                    'environment_id',
                    'name'
                ]}
            ]}
        ]
        logger.info('Fetching devices with specified device characteristics')
        devices=honeycomb_io.core.search_objects(
            object_name='Device',
            query_list=query_list,
            return_data=return_data,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
        logger.info('Fetched {} devices with specified device characteristics'.format(
            len(devices)
        ))
        logger.info('Filtering based on specified assignment characteristics')
        filtered_devices = list(filter(
            lambda device: len(honeycomb_io.environments.filter_assignments(
                assignments=device.get('assignments', []),
                environment_id=environment_id,
                environment_name=environment_name,
                start=start,
                end=end
            )) > 0,
            devices
        ))
        logger.info('Found {} devices with specified assignment characteristics'.format(
            len(filtered_devices)
        ))
        return_list = filtered_devices
    else:
        # No device characteristics were specified, so we search assignments instead
        if environment_id is None:
            if environment_name is not None:
                logger.info('Fetching environment ID for environment name \'{}\''.format(
                    environment_name
                ))
                environment_id = honeycomb_io.fetch_environment_id(
                    environment_name=environment_name,
                    client=None,
                    uri=None,
                    token_uri=None,
                    audience=None,
                    client_id=None,
                    client_secret=None
                )
        query_list = list()
        if environment_id is not None:
            query_list.append(
                {'field': 'environment', 'operator': 'EQ', 'value': environment_id}
            )
        if start is not None:
            query_list.append(
                {'operator': 'OR', 'children': [
                    {'field': 'end', 'operator': 'ISNULL'},
                    {'field': 'end', 'operator': 'GTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(start)}
                ]}
            )
        if end is not None:
            query_list.append(
                {'field': 'start', 'operator': 'LTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(end)}
            )
        if query_list is None:
            logger.warn('No criteria specified for device search. Returning no devices')
            return list()
        query_list.append(
            {'field': 'assigned_type', 'operator': 'EQ', 'value': 'DEVICE'}
        )
        return_data=[
            'assignment_id',
            'start',
            'end',
            {'environment': [
                'environment_id'
                'name'
            ]},
            {'assigned': [
                {'... on Device': [
                    'device_id',
                    'device_type',
                    'part_number',
                    'serial_number',
                    'tag_id',
                    'name'
                ]}
            ]}
        ]
        assignments = search_objects(
            object_name='Assignment',
            query_list=query_list,
            return_data=return_data,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
        device_dict = dict()
        for assignment in assignments:
            device_id = assignment.get('assigned').get('device_id')
            if assignment.get('assigned').get('device_id') not in device_dict.keys():
                device = assignment.get('assigned')
                assignment = {
                    'assignment_id': assignment.get('assignment_id'),
                    'start': assignment.get('start'),
                    'end': assignment.get('end'),
                    'environment': assignment.get('environment')
                }
                device['assignments'] = [assignment]
                device_dict[device_id] = device
            else:
                assignment = {
                    'assignment_id': assignment.get('assignment_id'),
                    'start': assignment.get('start'),
                    'end': assignment.get('end'),
                    'environment': assignment.get('environment')
                }
                device_dict[device_id]['assignments'].append(assignment)
        devices = list(device_dict.values())
        return_list = devices
    if output_format =='list':
        return return_list
    elif output_format == 'dataframe':
        return generate_device_dataframe(return_list)
    else:
        raise ValueError('Output format {} not recognized'.format(output_format))

def generate_device_dataframe(
    devices
):
    if len(devices) == 0:
        devices = [dict()]
    flat_list = list()
    for device in devices:
        flat_list.append({
            'device_id': device.get('device_id'),
            'device_type': device.get('device_type'),
            'device_part_number': device.get('part_number'),
            'device_serial_number': device.get('serial_number'),
            'device_tag_id': device.get('tag_id'),
            'device_name': device.get('name')
        })
    df = pd.DataFrame(flat_list, dtype='string')
    df.set_index('device_id', inplace=True)
    return df

def fetch_device_assignments_by_device_id(
    device_ids,
    start=None,
    end=None,
    require_unique_assignment=True,
    require_all_devices=True,
    output_format='list',
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    device_ids = list(pd.Series(device_ids).dropna())
    query_list = [
        {'field': 'assigned', 'operator': 'CONTAINED_BY', 'values': device_ids}
    ]
    if start is not None:
        query_list.append(
            {'operator': 'OR', 'children': [
                {'field': 'end', 'operator': 'ISNULL'},
                {'field': 'end', 'operator': 'GTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(start)}
            ]}
        )
    if end is not None:
        query_list.append(
            {'field': 'start', 'operator': 'LTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(end)}
        )
    return_data = [
        'assignment_id',
        {'assigned': [
            {'... on Device': [
                'device_id'
            ]}
        ]},
        'start',
        'end',
        {'environment': [
            'environment_id',
            'name'
        ]}
    ]
    if len(device_ids) > 0:
        assignments = honeycomb_io.core.search_objects(
            object_name='Assignment',
            query_list=query_list,
            return_data=return_data,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    else:
        assignments = []
    device_id_count = Counter([assignment.get('assigned', {}).get('device_id') for assignment in assignments])
    if require_unique_assignment:
        duplicate_device_ids = list()
        for device_id in device_id_count.keys():
            if device_id_count.get(device_id) > 1:
                duplicate_device_ids.append(device_id)
        if len(duplicate_device_ids) > 0:
            raise ValueError('Device IDs {} have more than one assignment in the specified time period'.format(
                duplicate_device_ids
            ))
    if require_all_devices:
        missing_device_ids = set(device_ids) - set(device_id_count.keys())
        if len(missing_device_ids) > 0:
            raise ValueError('Device IDs {} have no assignments in the specified time period'.format(
                list(missing_device_ids)
            ))
    if output_format =='list':
        return assignments
    elif output_format == 'dataframe':
        return generate_device_assignment_dataframe(assignments)
    else:
        raise ValueError('Output format {} not recognized'.format(output_format))

def generate_device_assignment_dataframe(
    assignments
):
    if len(assignments) == 0:
        assignments = [dict()]
    flat_list = list()
    for assignment in assignments:
        flat_list.append({
            'assignment_id': assignment.get('assignment_id'),
            'device_id': assignment.get('assigned', {}).get('device_id'),
            'assignment_start': pd.to_datetime(assignment.get('start'), utc=True),
            'assignment_end': pd.to_datetime(assignment.get('end'), utc=True),
            'environment_id': assignment.get('environment', {}).get('environment_id'),
            'environment_name': assignment.get('environment', {}).get('name')
        })
    df = pd.DataFrame(flat_list, dtype='object')
    df['assignment_start'] = pd.to_datetime(df['assignment_start'])
    df['assignment_end'] = pd.to_datetime(df['assignment_end'])
    df = df.astype({
        'assignment_id': 'string',
        'device_id': 'string',
        'environment_id': 'string',
        'environment_name': 'string'
    })
    df.set_index('assignment_id', inplace=True)
    return df

def fetch_device_entity_assignments_by_device_id(
    device_ids,
    start=None,
    end=None,
    require_unique_assignment=True,
    require_all_devices=False,
    output_format='list',
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    device_ids = list(pd.Series(device_ids).dropna())
    query_list = [
        {'field': 'device', 'operator': 'CONTAINED_BY', 'values': device_ids}
    ]
    if start is not None:
        query_list.append(
            {'operator': 'OR', 'children': [
                {'field': 'end', 'operator': 'ISNULL'},
                {'field': 'end', 'operator': 'GTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(start)}
            ]}
        )
    if end is not None:
        query_list.append(
            {'field': 'start', 'operator': 'LTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(end)}
        )
    return_data = [
        'entity_assignment_id',
        {'device': [
            'device_id'
        ]},
        'start',
        'end',
        {'entity': [
            'entity_type: __typename',
            {'... on Tray': [
                'tray_id',
                'name',
                'part_number',
                'serial_number'
            ]},
            {'... on Person': [
                'person_id',
                'person_type',
                'name',
                'first_name',
                'last_name',
                'nickname',
                'short_name',
                'transparent_classroom_id'
            ]},
        ]}
    ]
    if len(device_ids) > 0:
        entity_assignments = honeycomb_io.core.search_objects(
            object_name='EntityAssignment',
            query_list=query_list,
            return_data=return_data,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    else:
        entity_assignments = []
    device_id_count = Counter([entity_assignment.get('device', {}).get('device_id') for entity_assignment in entity_assignments])
    if require_unique_assignment:
        duplicate_device_ids = list()
        for device_id in device_id_count.keys():
            if device_id_count.get(device_id) > 1:
                duplicate_device_ids.append(device_id)
        if len(duplicate_device_ids) > 0:
            raise ValueError('Device IDs {} have more than one assignment in the specified time period'.format(
                duplicate_device_ids
            ))
    if require_all_devices:
        missing_device_ids = set(device_ids) - set(device_id_count.keys())
        if len(missing_device_ids) > 0:
            raise ValueError('Device IDs {} have no assignments in the specified time period'.format(
                list(missing_device_ids)
            ))
    if output_format =='list':
        return entity_assignments
    elif output_format == 'dataframe':
        return generate_device_entity_assignment_dataframe(entity_assignments)
    else:
        raise ValueError('Output format {} not recognized'.format(output_format))

def generate_device_entity_assignment_dataframe(
    entity_assignments
):
    if len(entity_assignments) == 0:
        entity_assignments = [dict()]
    flat_list = list()
    for entity_assignment in entity_assignments:
        entity_type = entity_assignment.get('entity', {}).get('entity_type', '')
        flat_list.append({
            'entity_assignment_id': entity_assignment.get('entity_assignment_id'),
            'device_id': entity_assignment.get('device', {}).get('device_id'),
            'entity_assignment_start': pd.to_datetime(entity_assignment.get('start'), utc=True),
            'entity_assignment_end': pd.to_datetime(entity_assignment.get('end'), utc=True),
            'entity_type': entity_assignment.get('entity', {}).get('entity_type'),
            'tray_id': entity_assignment.get('entity', {}).get('tray_id'),
            'tray_name': entity_assignment.get('entity', {}).get('name') if entity_type.upper() == 'TRAY' else None,
            'tray_part_number': entity_assignment.get('entity', {}).get('part_number'),
            'tray_serial_number': entity_assignment.get('entity', {}).get('serial_number'),
            'person_id': entity_assignment.get('entity', {}).get('person_id'),
            'person_type': entity_assignment.get('entity', {}).get('person_type'),
            'person_name': entity_assignment.get('entity', {}).get('name') if entity_type.upper() == 'PERSON' else None,
            'person_first_name': entity_assignment.get('entity', {}).get('first_name'),
            'person_last_name': entity_assignment.get('entity', {}).get('last_name'),
            'person_nickname': entity_assignment.get('entity', {}).get('nickname'),
            'person_short_name': entity_assignment.get('entity', {}).get('short_name'),
            'person_transparent_classroom_id': entity_assignment.get('entity', {}).get('transparent_classroom_id')
        })
    df = pd.DataFrame(flat_list, dtype='object')
    df['entity_assignment_start'] = pd.to_datetime(df['entity_assignment_start'])
    df['entity_assignment_end'] = pd.to_datetime(df['entity_assignment_end'])
    df = df.astype({
        'entity_assignment_id': 'string',
        'device_id': 'string',
        'entity_type': 'string',
        'tray_id': 'string',
        'tray_name': 'string',
        'tray_part_number': 'string',
        'tray_serial_number': 'string',
        'person_id': 'string',
        'person_type': 'string',
        'person_name': 'string',
        'person_first_name': 'string',
        'person_last_name': 'string',
        'person_nickname': 'string',
        'person_short_name': 'string',
        'person_transparent_classroom_id': 'Int64'
    })
    df.set_index('entity_assignment_id', inplace=True)
    return df


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
