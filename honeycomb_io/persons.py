import honeycomb_io.core
import minimal_honeycomb
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def fetch_persons(
    person_ids=None,
    person_types=None,
    names=None,
    first_names=None,
    last_names=None,
    nicknames=None,
    short_names=None,
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
        person_ids is not None or
        person_types is not None or
        names is not None or
        first_names is not None or
        last_names is not None or
        nicknames is not None or
        short_names is not None
    ):
        query_list = list()
        if person_ids is not None:
            query_list.append(
                {'field': 'person_id', 'operator': 'CONTAINED_BY', 'values': person_ids}
            )
        if person_types is not None:
            query_list.append(
                {'field': 'person_type', 'operator': 'CONTAINED_BY', 'values': person_types}
            )
        if names is not None:
            query_list.append(
                {'field': 'name', 'operator': 'CONTAINED_BY', 'values': names}
            )
        if first_names is not None:
            query_list.append(
                {'field': 'first_name', 'operator': 'CONTAINED_BY', 'values': first_names}
            )
        if last_names is not None:
            query_list.append(
                {'field': 'last_name', 'operator': 'CONTAINED_BY', 'values': last_names}
            )
        if nicknames is not None:
            query_list.append(
                {'field': 'nickname', 'operator': 'CONTAINED_BY', 'values': nicknames}
            )
        if short_names is not None:
            query_list.append(
                {'field': 'short_name', 'operator': 'CONTAINED_BY', 'values': short_names}
            )
        return_data = [
            'person_id',
            'person_type',
            'name',
            'first_name',
            'last_name',
            'nickname',
            'short_name',
            'transparent_classroom_id',
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
        logger.info('Fetching persons with specified person characteristics')
        persons=honeycomb_io.core.search_objects(
            object_name='Person',
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
        logger.info('Fetched {} persons with specified person characteristics'.format(
            len(persons)
        ))
        logger.info('Filtering based on specified assignment characteristics')
        filtered_persons = list(filter(
            lambda person: len(honeycomb_io.environments.filter_assignments(
                assignments=person.get('assignments', []),
                environment_id=environment_id,
                environment_name=environment_name,
                start=start,
                end=end
            )) > 0,
            persons
        ))
        logger.info('Found {} persons with specified assignment characteristics'.format(
            len(filtered_persons)
        ))
        return_list = filtered_persons
    else:
        # No person characteristics were specified, so we search assignments instead
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
            logger.warn('No criteria specified for person search. Returning no persons')
            return list()
        query_list.append(
            {'field': 'assigned_type', 'operator': 'EQ', 'value': 'PERSON'}
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
                {'... on Person': [
                    'person_id',
                    'person_type',
                    'name',
                    'first_name',
                    'last_name',
                    'nickname',
                    'short_name',
                    'transparent_classroom_id'
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
        person_dict = dict()
        for assignment in assignments:
            person_id = assignment.get('assigned').get('person_id')
            if person_id not in person_dict.keys():
                person = assignment.get('assigned')
                assignment = {
                    'assignment_id': assignment.get('assignment_id'),
                    'start': assignment.get('start'),
                    'end': assignment.get('end'),
                    'environment': assignment.get('environment')
                }
                person['assignments'] = [assignment]
                person_dict[person_id] = person
            else:
                assignment = {
                    'assignment_id': assignment.get('assignment_id'),
                    'start': assignment.get('start'),
                    'end': assignment.get('end'),
                    'environment': assignment.get('environment')
                }
                person_dict[person_id]['assignments'].append(assignment)
        persons = list(person_dict.values())
        return_list = persons
    if output_format =='list':
        return return_list
    elif output_format == 'dataframe':
        return generate_person_dataframe(return_list)
    else:
        raise ValueError('Output format {} not recognized'.format(output_format))

def generate_person_dataframe(
    persons
):
    if len(persons) == 0:
        persons = [dict()]
    flat_list = list()
    for person in persons:
        flat_list.append({
            'person_id': person.get('person_id'),
            'person_type': person.get('person_type'),
            'name': person.get('name'),
            'first_name': person.get('first_name'),
            'last_name': person.get('last_name'),
            'nickname': person.get('nickname'),
            'short_name': person.get('short_name'),
            'transparent_classroom_id': person.get('transparent_classroom_id')
        })
    df = pd.DataFrame(flat_list, dtype='string')
    df.set_index('person_id', inplace=True)
    return df


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
