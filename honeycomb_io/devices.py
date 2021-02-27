import minimal_honeycomb
import pandas as pd
import logging

logger = logging.getLogger(__name__)

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
