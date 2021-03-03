import minimal_honeycomb
import pandas as pd
import logging

logger = logging.getLogger(__name__)

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
