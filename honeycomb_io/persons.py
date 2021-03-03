import honeycomb_io.core
import minimal_honeycomb
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

# Used by:
# process_pose_data.local_io (wf-process-pose-data)
def fetch_person_info(
    environment_id,
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
    result = client.bulk_query(
        request_name='findAssignments',
        arguments={
            'environment': {
                'type': 'ID',
                'value': environment_id
            },
            'assigned_type': {
                'type': 'AssignableTypeEnum',
                'value': 'PERSON'
            },
        },
        return_data=[
            'assignment_id',
            {'assigned': [
                {'... on Person': [
                    'person_id',
                    'name',
                    'short_name'
                ]}
            ]}
        ],
        id_field_name='assignment_id'
    )
    data_list = list()
    for assignment in result:
        data_list.append({
            'person_id': assignment.get('assigned', {}).get('person_id'),
            'name': assignment.get('assigned', {}).get('name'),
            'short_name': assignment.get('assigned', {}).get('short_name')
        })
    person_info_df = pd.DataFrame(data_list)
    person_info_df.set_index('person_id', inplace=True)
    return person_info_df
