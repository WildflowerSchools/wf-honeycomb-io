import honeycomb_io.core
# import honeycomb_io.utils
# import minimal_honeycomb
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def fetch_all_coordinate_spaces(
    output_format='list',
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    return_data = [
        'space_id',
        'name',
        'axis_names',
        'origin_description',
        'axis_descriptions',
        {'environment':[
            'environment_id',
            'name'
        ]},
        'start',
        'end'
    ]
    logger.info('Fetching all coordinate spaces')
    coordinate_spaces = honeycomb_io.core.fetch_all_objects(
        object_name='CoordinateSpace',
        return_data=return_data,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    logger.info('Fetched {} coordinate spaces'.format(
        len(coordinate_spaces)
    ))
    if output_format =='list':
        return coordinate_spaces
    elif output_format == 'dataframe':
        return generate_coordinate_space_dataframe(coordinate_spaces)
    else:
        raise ValueError('Output format {} not recognized'.format(output_format))

def generate_coordinate_space_dataframe(
    coordinate_spaces
):
    if len(coordinate_spaces) == 0:
        coordinate_spaces = [dict()]
    flat_list = list()
    for coordinate_space in coordinate_spaces:
        flat_list.append({
            'coordinate_space_id': coordinate_space.get('space_id'),
            'coordinate_space_name': coordinate_space.get('name'),
            'coordinate_space_axis_names': coordinate_space.get('axis_names'),
            'coordinate_space_origin_description': coordinate_space.get('origin_description'),
            'enviroment_id': coordinate_space.get('environment', {}).get('environment_id'),
            'enviroment_name': coordinate_space.get('environment', {}).get('name'),
            'start': coordinate_space.get('start'),
            'end': coordinate_space.get('end')
        })
    df = pd.DataFrame(flat_list, dtype='string')
    df.set_index('coordinate_space_id', inplace=True)
    return df
