import honeycomb_io.utils
# import minimal_honeycomb
import pandas as pd
# from collections import Counter
import logging

logger = logging.getLogger(__name__)

def fetch_material_interactions(
    start=None,
    end=None,
    material_interaction_ids=None,
    source_types=None,
    person_ids=None,
    material_ids=None,
    output_format='list',
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    query_list = list()
    if start is not None:
        query_list.append(
            {'field': 'start', 'operator': 'GTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(start)}
        )
    if end is not None:
        query_list.append(
            {'field': 'start', 'operator': 'LTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(end)}
        )
    if material_interaction_ids is not None:
        query_list.append(
            {'field': 'material_interaction_id', 'operator': 'CONTAINED_BY', 'values': material_interaction_ids}
        )
    if source_types is not None:
        query_list.append(
            {'field': 'source_type', 'operator': 'CONTAINED_BY', 'values': source_types}
        )
    if person_ids is not None:
        query_list.append(
            {'field': 'person_id', 'operator': 'CONTAINED_BY', 'values': person_ids}
        )
    if material_ids is not None:
        query_list.append(
            {'field': 'material_id', 'operator': 'CONTAINED_BY', 'values': material_ids}
        )
    return_data = [
        'material_interaction_id',
        'start',
        'end',
        'source_type',
        {'person': [
            'person_id',
            'short_name'
        ]},
        {'material': [
            'material_id',
            'name'
        ]}
    ]
    logger.info('Fetching material interactions with specified material interaction characteristics')
    material_interactions=honeycomb_io.core.search_objects(
        object_name='MaterialInteraction',
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
    logger.info('Fetched {} material interactions with specified material interaction characteristics'.format(
        len(material_interactions)
    ))
    if output_format =='list':
        return material_interactions
    elif output_format == 'dataframe':
        return generate_material_interaction_dataframe(material_interactions)
    else:
        raise ValueError('Output format {} not recognized'.format(output_format))

def generate_material_interaction_dataframe(
    material_interactions
):
    if len(material_interactions) == 0:
        material_interactions = [dict()]
    flat_list = list()
    for material_interaction in material_interactions:
        flat_list.append({
            'material_interaction_id': material_interaction.get('material_interaction_id'),
            'start': material_interaction.get('start'),
            'end': material_interaction.get('end'),
            'source_type': material_interaction.get('source_type'),
            'person_id': material_interaction.get('person', {}).get('person_id'),
            'person_short_name': material_interaction.get('person', {}).get('short_name'),
            'material_id': material_interaction.get('material', {}).get('material_id'),
            'material_name': material_interaction.get('material', {}).get('name'),
        })
    df = pd.DataFrame(flat_list, dtype='string')
    df['start'] = pd.to_datetime(df['start'])
    df['end'] = pd.to_datetime(df['end'])
    df.set_index('material_interaction_id', inplace=True)
    return df
