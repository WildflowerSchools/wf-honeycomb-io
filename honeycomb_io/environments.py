import honeycomb_io.core
import minimal_honeycomb
import pandas as pd
import logging

logger = logging.getLogger(__name__)

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
