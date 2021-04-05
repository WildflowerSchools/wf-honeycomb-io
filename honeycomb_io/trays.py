import honeycomb_io.utils
import minimal_honeycomb
import pandas as pd
from collections import Counter
import logging

logger = logging.getLogger(__name__)

def fetch_trays(
    tray_ids=None,
    part_numbers=None,
    serial_numbers=None,
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
        tray_ids is not None or
        part_numbers is not None or
        serial_numbers is not None or
        names is not None
    ):
        query_list = list()
        if tray_ids is not None:
            query_list.append(
                {'field': 'tray_id', 'operator': 'CONTAINED_BY', 'values': tray_ids}
            )
        if part_numbers is not None:
            query_list.append(
                {'field': 'part_number', 'operator': 'CONTAINED_BY', 'values': part_numbers}
            )
        if serial_numbers is not None:
            query_list.append(
                {'field': 'serial_number', 'operator': 'CONTAINED_BY', 'values': serial_numbers}
            )
        if names is not None:
            query_list.append(
                {'field': 'name', 'operator': 'CONTAINED_BY', 'values': names}
            )
        return_data = [
            'tray_id',
            'part_number',
            'serial_number',
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
        logger.info('Fetching trays with specified tray characteristics')
        trays=honeycomb_io.core.search_objects(
            object_name='Tray',
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
        logger.info('Fetched {} trays with specified tray characteristics'.format(
            len(trays)
        ))
        logger.info('Filtering based on specified assignment characteristics')
        filtered_trays = list(filter(
            lambda tray: len(honeycomb_io.environments.filter_assignments(
                assignments=tray.get('assignments', []),
                environment_id=environment_id,
                environment_name=environment_name,
                start=start,
                end=end
            )) > 0,
            trays
        ))
        logger.info('Found {} trays with specified assignment characteristics'.format(
            len(filtered_trays)
        ))
        return_list = filtered_trays
    else:
        # No tray characteristics were specified, so we search assignments instead
        if environment_id is None:
            if environment_name is not None:
                logger.info('Fetching environment ID for environment name \'{}\''.format(
                    environment_name
                ))
                environment_id = honeycomb_io.fetch_environment_id(
                    environment_name=environment_name,
                    client=client,
                    uri=uri,
                    token_uri=token_uri,
                    audience=audience,
                    client_id=client_id,
                    client_secret=client_secret
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
            logger.warn('No criteria specified for tray search. Returning no trays')
            return list()
        query_list.append(
            {'field': 'assigned_type', 'operator': 'EQ', 'value': 'TRAY'}
        )
        return_data=[
            'assignment_id',
            'start',
            'end',
            {'environment': [
                'environment_id',
                'name'
            ]},
            {'assigned': [
                {'... on Tray': [
                    'tray_id',
                    'part_number',
                    'serial_number',
                    'name'
                ]}
            ]}
        ]
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
        tray_dict = dict()
        for assignment in assignments:
            tray_id = assignment.get('assigned').get('tray_id')
            if tray_id not in tray_dict.keys():
                tray = assignment.get('assigned')
                assignment = {
                    'assignment_id': assignment.get('assignment_id'),
                    'start': assignment.get('start'),
                    'end': assignment.get('end'),
                    'environment': assignment.get('environment')
                }
                tray['assignments'] = [assignment]
                tray_dict[tray_id] = tray
            else:
                assignment = {
                    'assignment_id': assignment.get('assignment_id'),
                    'start': assignment.get('start'),
                    'end': assignment.get('end'),
                    'environment': assignment.get('environment')
                }
                tray_dict[tray_id]['assignments'].append(assignment)
        trays = list(tray_dict.values())
        return_list = trays
    if output_format =='list':
        return return_list
    elif output_format == 'dataframe':
        return generate_tray_dataframe(return_list)
    else:
        raise ValueError('Output format {} not recognized'.format(output_format))

def generate_tray_dataframe(
    trays
):
    if len(trays) == 0:
        trays = [dict()]
    flat_list = list()
    for tray in trays:
        flat_list.append({
            'tray_id': tray.get('tray_id'),
            'tray_part_number': tray.get('part_number'),
            'tray_serial_number': tray.get('serial_number'),
            'tray_name': tray.get('name')
        })
    df = pd.DataFrame(flat_list, dtype='string')
    df.set_index('tray_id', inplace=True)
    return df


# Not currently used
def fetch_tray_ids():
    logger.info('Fetching entity assignment info to extract tray IDs')
    client = minimal_honeycomb.MinimalHoneycombClient()
    result = client.request(
        request_type="query",
        request_name='entityAssignments',
        arguments=None,
        return_object=[
            {'data': [
                'entity_assignment_id',
                {'entity': [
                    {'... on Tray': [
                        'tray_id'
                    ]}
                ]}
            ]}
        ]
    )
    df = pd.json_normalize(result.get('data'))
    df.rename(
        columns={
            'entity.tray_id': 'tray_id',
        },
        inplace=True
    )
    logger.info(
        'Found {} entity assignments for trays'.format(
            df['tray_id'].notna().sum()))
    df.set_index('entity_assignment_id', inplace=True)
    return df

def fetch_tray_material_assignments_by_tray_id(
    tray_ids,
    start=None,
    end=None,
    require_unique_assignment=True,
    require_all_trays=False,
    output_format='list',
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    tray_ids = list(pd.Series(tray_ids).dropna())
    query_list = [
        {'field': 'tray', 'operator': 'CONTAINED_BY', 'values': tray_ids}
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
        'material_assignment_id',
        {'tray': [
            'tray_id'
        ]},
        'start',
        'end',
        {'material': [
            'material_id',
            'name',
            'transparent_classroom_id',
            'transparent_classroom_type'
        ]}
    ]
    if len(tray_ids) > 0:
        material_assignments = honeycomb_io.core.search_objects(
            object_name='MaterialAssignment',
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
        material_assignments = []
    tray_id_count = Counter([material_assignment.get('tray', {}).get('tray_id') for material_assignment in material_assignments])
    if require_unique_assignment:
        duplicate_tray_ids = list()
        for tray_id in tray_id_count.keys():
            if tray_id_count.get(tray_id) > 1:
                duplicate_tray_ids.append(tray_id)
        if len(duplicate_tray_ids) > 0:
            raise ValueError('Tray IDs {} have more than one assignment in the specified time period'.format(
                duplicate_tray_ids
            ))
    if require_all_trays:
        missing_tray_ids = set(tray_ids) - set(tray_id_count.keys())
        if len(missing_tray_ids) > 0:
            raise ValueError('Tray IDs {} have no assignments in the specified time period'.format(
                list(missing_tray_ids)
            ))
    if output_format =='list':
        return material_assignments
    elif output_format == 'dataframe':
        return generate_tray_material_assignment_dataframe(material_assignments)
    else:
        raise ValueError('Output format {} not recognized'.format(output_format))

def generate_tray_material_assignment_dataframe(
    material_assignments
):
    if len(material_assignments) == 0:
        material_assignments = [dict()]
    flat_list = list()
    for material_assignment in material_assignments:
        flat_list.append({
            'material_assignment_id': material_assignment.get('material_assignment_id'),
            'tray_id': material_assignment.get('tray', {}).get('tray_id'),
            'material_assignment_start': pd.to_datetime(material_assignment.get('start'), utc=True),
            'material_assignment_end': pd.to_datetime(material_assignment.get('end'), utc=True),
            'material_id': material_assignment.get('material', {}).get('material_id'),
            'material_name': material_assignment.get('material', {}).get('name'),
            'material_transparent_classroom_id': material_assignment.get('material', {}).get('transparent_classroom_id'),
            'material_transparent_classroom_type': material_assignment.get('material', {}).get('transparent_classroom_type')
        })
    df = pd.DataFrame(flat_list, dtype='object')
    df['material_assignment_start'] = pd.to_datetime(df['material_assignment_start'])
    df['material_assignment_end'] = pd.to_datetime(df['material_assignment_end'])
    df = df.astype({
        'material_assignment_id': 'string',
        'tray_id': 'string',
        'material_id': 'string',
        'material_name': 'string',
        'material_transparent_classroom_id':'Int64',
        'material_transparent_classroom_type': 'string'
    })
    df.set_index('material_assignment_id', inplace=True)
    return df
