import minimal_honeycomb
import pandas as pd
import logging

logger = logging.getLogger(__name__)

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
