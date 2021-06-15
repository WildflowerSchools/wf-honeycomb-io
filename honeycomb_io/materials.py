import honeycomb_io.core
import minimal_honeycomb
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def fetch_materials(
    material_ids=None,
    names=None,
    transparent_classroom_ids=None,
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
        material_ids is not None or
        names is not None or
        transparent_classroom_ids is not None
    ):
        query_list = list()
        if material_ids is not None:
            query_list.append(
                {'field': 'material_id', 'operator': 'CONTAINED_BY', 'values': material_ids}
            )
        if names is not None:
            query_list.append(
                {'field': 'name', 'operator': 'CONTAINED_BY', 'values': names}
            )
        if transparent_classroom_ids is not None:
            query_list.append(
                {'field': 'transparent_classroom_id', 'operator': 'CONTAINED_BY', 'values': transparent_classroom_ids}
            )
        return_data = [
            'material_id',
            'name',
            'transparent_classroom_id',
            'transparent_classroom_type',
            'description',
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
        logger.info('Fetching materials with specified material characteristics')
        materials=honeycomb_io.core.search_objects(
            object_name='Material',
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
        logger.info('Fetched {} materials with specified material characteristics'.format(
            len(materials)
        ))
        logger.info('Filtering based on specified assignment characteristics')
        filtered_materials = list(filter(
            lambda material: len(honeycomb_io.environments.filter_assignments(
                assignments=material.get('assignments', []),
                environment_id=environment_id,
                environment_name=environment_name,
                start=start,
                end=end
            )) > 0,
            materials
        ))
        logger.info('Found {} materials with specified assignment characteristics'.format(
            len(filtered_materials)
        ))
        return_list = filtered_materials
    else:
        # No material characteristics were specified, so we search assignments instead
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
            logger.warn('No criteria specified for material search. Returning no materials')
            return list()
        query_list.append(
            {'field': 'assigned_type', 'operator': 'EQ', 'value': 'MATERIAL'}
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
                {'... on Material': [
                    'material_id',
                    'name',
                    'transparent_classroom_id',
                    'transparent_classroom_type',
                    'description'
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
        material_dict = dict()
        for assignment in assignments:
            material_id = assignment.get('assigned').get('material_id')
            if material_id not in material_dict.keys():
                material = assignment.get('assigned')
                assignment = {
                    'assignment_id': assignment.get('assignment_id'),
                    'start': assignment.get('start'),
                    'end': assignment.get('end'),
                    'environment': assignment.get('environment')
                }
                material['assignments'] = [assignment]
                material_dict[material_id] = material
            else:
                assignment = {
                    'assignment_id': assignment.get('assignment_id'),
                    'start': assignment.get('start'),
                    'end': assignment.get('end'),
                    'environment': assignment.get('environment')
                }
                material_dict[material_id]['assignments'].append(assignment)
        materials = list(material_dict.values())
        return_list = materials
    if output_format =='list':
        return return_list
    elif output_format == 'dataframe':
        return generate_material_dataframe(return_list)
    else:
        raise ValueError('Output format {} not recognized'.format(output_format))

def generate_material_dataframe(
    materials
):
    if len(materials) == 0:
        materials = [dict()]
    flat_list = list()
    for material in materials:
        flat_list.append({
            'material_id': material.get('material_id'),
            'material_name': material.get('name'),
            'material_transparent_classroom_id': material.get('transparent_classroom_id'),
            'material_transparent_classroom_type': material.get('transparent_classroom_type'),
            'material_description': material.get('description')
        })
    df = pd.DataFrame(flat_list, dtype='string')
    df['material_transparent_classroom_id'] = pd.to_numeric(df['material_transparent_classroom_id']).astype('Int64')
    df.set_index('material_id', inplace=True)
    return df

# Used by:
# honeycomb_io.uwb_data
def fetch_material_names(
):
    logger.info('Fetching material assignment info to extract material names')
    client = minimal_honeycomb.MinimalHoneycombClient()
    result = client.request(
        request_type="query",
        request_name='materialAssignments',
        arguments=None,
        return_object=[
            {'data': [
                'material_assignment_id',
                {'material': [
                    'material_id',
                    'material_name: name'
                ]}
            ]}
        ]
    )
    df = pd.json_normalize(result.get('data'))
    df.rename(
        columns={
            'material.material_id': 'material_id',
            'material.material_name': 'material_name'
        },
        inplace=True
    )
    df.set_index('material_assignment_id', inplace=True)
    logger.info('Found {} material assignments'.format(
        df['material_id'].notna().sum()
    ))
    return df

# Used by:
# honeycomb_io.uwb_data
def fetch_material_assignments():
    logger.info('Fetching material assignment IDs')
    client = minimal_honeycomb.MinimalHoneycombClient()
    result = client.request(
        request_type="query",
        request_name='materialAssignments',
        arguments=None,
        return_object=[
            {'data': [
                'material_assignment_id',
                {'tray': [
                    'tray_id'
                ]},
                'start',
                'end'
            ]}
        ]
    )
    if len(result.get('data')) == 0:
        raise ValueError('No material assignments found')
    logger.info('Found {} material assignments'.format(
        len(result.get('data'))))
    assignments_dict = dict()
    for material_assignment in result.get('data'):
        tray_id = material_assignment['tray']['tray_id']
        assignment = {
            'material_assignment_id': material_assignment['material_assignment_id'],
            'start': material_assignment['start'],
            'end': material_assignment['end']
        }
        if tray_id in assignments_dict.keys():
            assignments_dict[tray_id].append(assignment)
        else:
            assignments_dict[tray_id] = [assignment]
    for tray_id in assignments_dict.keys():
        num_assignments = len(assignments_dict[tray_id])
        # Convert timestamp strings to Pandas datetime objects
        for assignment_index in range(num_assignments):
            assignments_dict[tray_id][assignment_index]['start'] = pd.to_datetime(
                assignments_dict[tray_id][assignment_index]['start'],
                utc=True
            )
            assignments_dict[tray_id][assignment_index]['end'] = pd.to_datetime(
                assignments_dict[tray_id][assignment_index]['end'],
                utc=True
            )
        # Sort assignment list by start time
        assignments_dict[tray_id] = sorted(
            assignments_dict[tray_id],
            key=lambda assignment: assignment['start']
        )
        # Check integrity of assignment list
        if num_assignments > 1:
            for assignment_index in range(1, num_assignments):
                if pd.isna(assignments_dict[tray_id]
                           [assignment_index - 1]['end']):
                    raise ValueError('Assignment {} starts at {} but previous assignment for this device {} starts at {} and has no end time'.format(
                        assignments_dict[tray_id][assignment_index]['material_assignment_id'],
                        assignments_dict[tray_id][assignment_index]['start'],
                        assignments_dict[tray_id][assignment_index -
                                                  1]['material_assignment_id'],
                        assignments_dict[tray_id][assignment_index - 1]['start']
                    ))
                if assignments_dict[tray_id][assignment_index]['start'] < assignments_dict[tray_id][assignment_index - 1]['end']:
                    raise ValueError('Assignment {} starts at {} but previous assignment for this device {} starts at {} and ends at {}'.format(
                        assignments_dict[tray_id][assignment_index]['material_assignment_id'],
                        assignments_dict[tray_id][assignment_index]['start'],
                        assignments_dict[tray_id][assignment_index -
                                                  1]['material_assignment_id'],
                        assignments_dict[tray_id][assignment_index - 1]['start'],
                        assignments_dict[tray_id][assignment_index - 1]['end']
                    ))
    return assignments_dict
