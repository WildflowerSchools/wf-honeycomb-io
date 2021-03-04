import honeycomb_io.core
import honeycomb_io.utils
import honeycomb_io.cameras
import minimal_honeycomb
import pandas as pd
import numpy as np
import datetime
import logging

logger = logging.getLogger(__name__)

# The following functions are used by process_pose_data.geom_render
# (wf-process-pose_data) but not implemented here:
# fetch_2d_pose_data_by_inference_execution()
# fetch_2d_pose_data_by_time_span()
# extract_pose_model_id()
# fetch_pose_model_info()

# Not currently used
def fetch_2d_pose_data(
    start=None,
    end=None,
    environment_id=None,
    environment_name=None,
    camera_ids=None,
    camera_device_types=None,
    camera_part_numbers=None,
    camera_names=None,
    camera_serial_numbers=None,
    pose_model_id=None,
    pose_model_name=None,
    pose_model_variant_name=None,
    inference_ids=None,
    inference_names=None,
    inference_models=None,
    inference_versions=None,
    return_track_label=False,
    return_person_id=False,
    return_inference_id=False,
    return_pose_model_id=True,
    return_pose_quality=False,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    camera_ids_from_environment = honeycomb_io.cameras.fetch_camera_ids_from_environment(
        start=start,
        end=end,
        environment_id=environment_id,
        environment_name=environment_name,
        camera_device_types=camera_device_types,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    camera_ids_from_camera_properties = honeycomb_io.cameras.fetch_camera_ids_from_camera_properties(
        camera_ids=camera_ids,
        camera_device_types=camera_device_types,
        camera_part_numbers=camera_part_numbers,
        camera_names=camera_names,
        camera_serial_numbers=camera_serial_numbers,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    pose_model_id = fetch_pose_model_id(
        pose_model_id=pose_model_id,
        pose_model_name=pose_model_name,
        pose_model_variant_name=pose_model_variant_name,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    inference_ids = honeycomb_io.inference_executions.fetch_inference_ids(
        inference_ids=inference_ids,
        inference_names=inference_names,
        inference_models=inference_models,
        inference_versions=inference_versions,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    logger.info('Building query list for 2D pose search')
    query_list = list()
    if start is not None:
        query_list.append({
            'field': 'timestamp',
            'operator': 'GTE',
            'value': honeycomb_io.utils.to_honeycomb_datetime(start)
        })
    if end is not None:
        query_list.append({
            'field': 'timestamp',
            'operator': 'LTE',
            'value': honeycomb_io.utils.to_honeycomb_datetime(end)
        })
    if camera_ids_from_environment is not None:
        query_list.append({
            'field': 'camera',
            'operator': 'IN',
            'values': camera_ids_from_environment
        })
    if camera_ids_from_camera_properties is not None:
        query_list.append({
            'field': 'camera',
            'operator': 'IN',
            'values': camera_ids_from_camera_properties
        })
    if pose_model_id is not None:
        query_list.append({
            'field': 'pose_model',
            'operator': 'EQ',
            'value': pose_model_id
        })
    if inference_ids is not None:
        query_list.append({
            'field': 'source',
            'operator': 'IN',
            'values': inference_ids
        })
    return_data= [
        'pose_id',
        'timestamp',
        {'camera': [
            'device_id'
        ]},
        'track_label',
        {'pose_model': [
            'pose_model_id'
        ]},
        {'keypoints': [
            'coordinates',
            'quality'
        ]},
        'quality',
        {'person': [
            'person_id'
        ]},
        {'source': [
            {'... on InferenceExecution': [
                'inference_id'
            ]}
        ]}
    ]
    result = search_2d_poses(
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    data = list()
    logger.info('Parsing {} returned poses'.format(len(result)))
    for datum in result:
        data.append({
            'pose_2d_id': datum.get('pose_id'),
            'timestamp': datum.get('timestamp'),
            'camera_id': (datum.get('camera') if datum.get('camera') is not None else {}).get('device_id'),
            'track_label_2d': datum.get('track_label'),
            'person_id': (datum.get('person') if datum.get('person') is not None else {}).get('person_id'),
            'inference_id': (datum.get('source') if datum.get('source') is not None else {}).get('inference_id'),
            'pose_model_id': (datum.get('pose_model') if datum.get('pose_model') is not None else {}).get('pose_model_id'),
            'keypoint_coordinates_2d': np.asarray([keypoint.get('coordinates') for keypoint in datum.get('keypoints')], dtype=np.float),
            'keypoint_quality_2d': np.asarray([keypoint.get('quality') for keypoint in datum.get('keypoints')], dtype=np.float),
            'pose_quality_2d': datum.get('quality')
        })
    poses_2d_df = pd.DataFrame(data)
    poses_2d_df['keypoint_coordinates_2d'] = poses_2d_df['keypoint_coordinates_2d'].apply(lambda x: np.where(x == 0.0, np.nan, x))
    poses_2d_df['timestamp'] = pd.to_datetime(poses_2d_df['timestamp'])
    if poses_2d_df['pose_model_id'].nunique() > 1:
        raise ValueError('Returned poses are associated with multiple pose models')
    if (poses_2d_df.groupby(['timestamp', 'camera_id'])['inference_id'].nunique() > 1).any():
        raise ValueError('Returned poses have multiple inference IDs for some camera IDs at some timestamps')
    poses_2d_df.set_index('pose_2d_id', inplace=True)
    return_columns = [
        'timestamp',
        'camera_id'
    ]
    if return_track_label:
        return_columns.append('track_label_2d')
    if return_person_id:
        return_columns.append('person_id')
    if return_inference_id:
        return_columns.append('inference_id')
    if return_pose_model_id:
        return_columns.append('pose_model_id')
    return_columns.extend([
        'keypoint_coordinates_2d',
        'keypoint_quality_2d'
    ])
    if return_pose_quality:
        return_columns.append('pose_quality_2d')
    poses_2d_df = poses_2d_df.reindex(columns=return_columns)
    return poses_2d_df

# Not currently used
def search_2d_poses(
    query_list,
    return_data,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Searching for 2D poses that match the specified parameters')
    result = honeycomb_io.core.search_objects(
        object_name='Pose2D',
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    logger.info('Fetched {} poses'.format(len(result)))
    return result

# Not currently used
def fetch_3d_pose_data(
    start=None,
    end=None,
    pose_model_id=None,
    pose_model_name=None,
    pose_model_variant_name=None,
    inference_ids=None,
    inference_names=None,
    inference_models=None,
    inference_versions=None,
    return_keypoint_quality=False,
    return_coordinate_space_id=False,
    return_track_label=False,
    return_poses_2d=True,
    return_person_id=False,
    return_inference_id=False,
    return_pose_model_id=False,
    return_pose_quality=False,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    pose_model_id = fetch_pose_model_id(
        pose_model_id=pose_model_id,
        pose_model_name=pose_model_name,
        pose_model_variant_name=pose_model_variant_name,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    inference_ids = honeycomb_io.inference_executions.fetch_inference_ids(
        inference_ids=inference_ids,
        inference_names=inference_names,
        inference_models=inference_models,
        inference_versions=inference_versions,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    logger.info('Building query list for 3D pose search')
    query_list = list()
    if start is not None:
        query_list.append({
            'field': 'timestamp',
            'operator': 'GTE',
            'value': honeycomb_io.utils.to_honeycomb_datetime(start)
        })
    if end is not None:
        query_list.append({
            'field': 'timestamp',
            'operator': 'LTE',
            'value': honeycomb_io.utils.to_honeycomb_datetime(end)
        })
    if pose_model_id is not None:
        query_list.append({
            'field': 'pose_model',
            'operator': 'EQ',
            'value': pose_model_id
        })
    if inference_ids is not None:
        query_list.append({
            'field': 'source',
            'operator': 'IN',
            'values': inference_ids
        })
    return_data= [
        'pose_id',
        'timestamp',
        'track_label',
        {'pose_model': [
            'pose_model_id'
        ]},
        {'keypoints': [
            'coordinates',
            'quality'
        ]},
        {'coordinate_space': [
            'space_id'
        ]},
        'quality',
        'poses_2d',
        {'person': [
            'person_id'
        ]},
        {'source': [
            {'... on InferenceExecution': [
                'inference_id'
            ]}
        ]}
    ]
    result = search_3d_poses(
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    data = list()
    logger.info('Parsing {} returned poses'.format(len(result)))
    for datum in result:
        data.append({
            'pose_3d_id': datum.get('pose_id'),
            'timestamp': datum.get('timestamp'),
            'track_label_3d': datum.get('track_label'),
            'pose_2d_ids': datum.get('poses_2d'),
            'person_id': (datum.get('person') if datum.get('person') is not None else {}).get('person_id'),
            'inference_id': (datum.get('source') if datum.get('source') is not None else {}).get('inference_id'),
            'pose_model_id': (datum.get('pose_model') if datum.get('pose_model') is not None else {}).get('pose_model_id'),
            'keypoint_coordinates_3d': np.asarray([keypoint.get('coordinates') for keypoint in datum.get('keypoints')], dtype=np.float),
            'keypoint_quality_3d': np.asarray([keypoint.get('quality') for keypoint in datum.get('keypoints')], dtype=np.float),
            'coordinate_space_id': datum.get('coordinate_space').get('space_id'),
            'pose_quality_3d': datum.get('quality')
        })
    poses_3d_df = pd.DataFrame(data)
    poses_3d_df['timestamp'] = pd.to_datetime(poses_3d_df['timestamp'])
    if poses_3d_df['pose_model_id'].nunique() > 1:
        raise ValueError('Returned poses are associated with multiple pose models')
    if (poses_3d_df.groupby('timestamp')['inference_id'].nunique() > 1).any():
        raise ValueError('Returned poses have multiple inference IDs for timestamps')
    poses_3d_df.set_index('pose_3d_id', inplace=True)
    return_columns = [
        'timestamp'
    ]
    if return_track_label:
        return_columns.append('track_label_3d')
    if return_poses_2d:
        return_columns.append('pose_2d_ids')
    if return_person_id:
        return_columns.append('person_id')
    if return_inference_id:
        return_columns.append('inference_id')
    if return_pose_model_id:
        return_columns.append('pose_model_id')
    return_columns.append('keypoint_coordinates_3d')
    if return_keypoint_quality:
        return_columns.append('keypoint_quality_3d')
    if return_pose_quality:
        return_columns.append('pose_quality_3d')
    if return_coordinate_space_id:
        return_columns.append('coordinate_space_id')
    poses_3d_df = poses_3d_df.reindex(columns=return_columns)
    return poses_3d_df

# Not currently used
def search_3d_poses(
    query_list,
    return_data,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Searching for 3D poses that match the specified parameters')
    result = honeycomb_io.core.search_objects(
        object_name='Pose3D',
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    logger.info('Fetched {} poses'.format(len(result)))
    return result

# Not currently used
def fetch_3d_pose_track_data(
    inference_ids=None,
    inference_names=None,
    inference_models=None,
    inference_versions=None,
    return_track_label=False,
    return_inference_id=False,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    inference_ids = honeycomb_io.inference_executions.fetch_inference_ids(
        inference_ids=inference_ids,
        inference_names=inference_names,
        inference_models=inference_models,
        inference_versions=inference_versions,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    logger.info('Building query list for 3D pose track search')
    query_list = list()
    if inference_ids is not None:
        query_list.append({
            'field': 'source',
            'operator': 'IN',
            'values': inference_ids
        })
    return_data = [
        'pose_track_id',
        'poses_3d',
        'track_label',
        {'source': [
            {'... on InferenceExecution': [
                'inference_id'
            ]}
        ]}
    ]
    result = search_pose_tracks_3d(
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    data = list()
    logger.info('Parsing {} returned pose tracks'.format(len(result)))
    for datum in result:
        data.append({
            'pose_track_3d_id': datum.get('pose_track_id'),
            'pose_3d_ids': datum.get('poses_3d'),
            'track_label_3d': datum.get('track_label'),
            'inference_id': (datum.get('source') if datum.get('source') is not None else {}).get('inference_id')
        })
    pose_tracks_3d_df = pd.DataFrame(data)
    pose_tracks_3d_df.set_index('pose_track_3d_id', inplace=True)
    return_columns = [
        'pose_3d_ids'
    ]
    if return_track_label:
        return_columns.append('track_label_3d')
    if return_inference_id:
        return_columns.append('inference_id')
    pose_tracks_3d_df = pose_tracks_3d_df.reindex(columns=return_columns)
    return pose_tracks_3d_df

# Not currently used
def search_pose_tracks_3d(
    query_list,
    return_data,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Searching for 3D pose tracks that match the specified parameters')
    result = honeycomb_io.core.search_objects(
        object_name='poseTrack3D',
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    logger.info('Fetched {} pose tracks'.format(len(result)))
    return result

# Not currently used
def fetch_pose_model_id(
    pose_model_id=None,
    pose_model_name=None,
    pose_model_variant_name=None,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if pose_model_id is not None:
        if pose_model_name is not None or pose_model_variant_name is not None:
            raise ValueError('If pose model ID is specified, pose model name/variant name cannot be specified')
        return pose_model_id
    if pose_model_name is not None or pose_model_variant_name is not None:
        arguments=dict()
        if pose_model_name is not None:
            arguments['model_name'] = {
                'type': 'String',
                'value': pose_model_name
            }
        if pose_model_variant_name is not None:
            arguments['model_variant_name'] = {
                'type': 'String',
                'value': pose_model_variant_name
            }
        logger.info('Fetching pose model ID for pose model with specified properties')
        client = honeycomb_io.core.generate_client(
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
        result = client.bulk_query(
            request_name='findPoseModels',
            arguments=arguments,
            return_data=[
                'pose_model_id'
            ],
            id_field_name='pose_model_id'
        )
        if len(result) == 0:
            raise ValueError('No pose models match specified model name/model variant name')
        if len(result) > 1:
            raise ValueError('Multiple pose models match specified model name/model variant name')
        pose_model_id = result[0].get('pose_model_id')
        logger.info('Found pose model ID for pose model with specified properties')
        return pose_model_id
    return None

# Used by:
# process_pose_data.overlay (wf-process-pose-data)
def fetch_pose_model(
    pose_2d_id,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Fetching pose model information for specified pose')
    client = honeycomb_io.core.generate_client(
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    result = client.request(
        request_type='query',
        request_name='getPose2D',
        arguments={
            'pose_id': {
                'type': 'ID!',
                'value': pose_2d_id
            }
        },
        return_object=[
            {'pose_model': [
                'pose_model_id',
                'model_name',
                'model_variant_name',
                'keypoint_names',
                'keypoint_descriptions',
                'keypoint_connectors'
            ]}
        ])
    pose_model = result.get('pose_model')
    return pose_model

# Used by:
# process_pose_data.overlay (wf-process-pose_data)
# process_pose_data.reconstruct (wf-process-pose-data)
def fetch_pose_model_by_pose_model_id(
    pose_model_id,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Fetching pose model information for specified pose model ID')
    client = honeycomb_io.core.generate_client(
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    result = client.request(
        request_type='query',
        request_name='getPoseModel',
        arguments={
            'pose_model_id': {
                'type': 'ID!',
                'value': pose_model_id
            }
        },
        return_object=[
            'pose_model_id',
            'model_name',
            'model_variant_name',
            'keypoint_names',
            'keypoint_descriptions',
            'keypoint_connectors'
        ])
    pose_model = result
    return pose_model

# Not currently used
def fetch_inference_ids_reconstruct_3d_poses(
    chunk_size=100,
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
        request_name='findInferenceExecutions',
        arguments={
            'name': {
                'type': 'String',
                'value': 'Reconstruct 3D poses from 2D poses'
            }
        },
        return_data=[
            'inference_id'
        ],
        id_field_name='inference_id'
    )
    inference_ids = [datum.get('inference_id') for datum in result]
    return inference_ids

# Not currently used
def write_3d_pose_data(
    poses_3d_df,
    coordinate_space_id=None,
    pose_model_id=None,
    source_id=None,
    source_type=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    poses_3d_df_honeycomb = poses_3d_df.copy()
    if coordinate_space_id is None:
        if 'coordinate_space_id' not in poses_3d_df_honeycomb.columns:
            raise ValueError('Coordinate space ID must either be included in data frame or specified')
    else:
        poses_3d_df_honeycomb['coordinate_space_id'] = coordinate_space_id
    if pose_model_id is None:
        if 'pose_model_id' not in poses_3d_df_honeycomb.columns:
            raise ValueError('Pose model ID must either be included in data frame or specified')
    else:
        poses_3d_df_honeycomb['pose_model_id'] = pose_model_id
    if source_id is None:
        if 'source_id' not in poses_3d_df_honeycomb.columns:
            raise ValueError('Source ID must either be included in data frame or specified')
    else:
        poses_3d_df_honeycomb['source_id'] = source_id
    if source_type is None:
        if 'source_type' not in poses_3d_df_honeycomb.columns:
            raise ValueError('Source type must either be included in data frame or specified')
    else:
        poses_3d_df_honeycomb['source_type'] = source_type
    poses_3d_df_honeycomb['timestamp'] = poses_3d_df_honeycomb['timestamp'].apply(
        lambda x: honeycomb_io.utils.to_honeycomb_datetime(x.to_pydatetime())
    )
    poses_3d_df_honeycomb['keypoint_coordinates_3d'] = poses_3d_df_honeycomb['keypoint_coordinates_3d'].apply(
        lambda x: np.where(np.isnan(x), None, x)
    )
    poses_3d_df_honeycomb['keypoint_coordinates_3d'] = poses_3d_df_honeycomb['keypoint_coordinates_3d'].apply(
        lambda x: [{'coordinates': x[i, :].tolist()} for i in range(x.shape[0])]
    )
    poses_3d_df_honeycomb = poses_3d_df_honeycomb.reindex(columns=[
        'timestamp',
        'coordinate_space_id',
        'pose_model_id',
        'keypoint_coordinates_3d',
        'pose_2d_ids',
        'source_id',
        'source_type'
    ])
    poses_3d_df_honeycomb.rename(
        columns={
            'coordinate_space_id': 'coordinate_space',
            'pose_model_id': 'pose_model',
            'keypoint_coordinates_3d': 'keypoints',
            'pose_2d_ids': 'poses_2d',
            'source_id': 'source'
        },
        inplace=True
    )
    poses_3d_list_honeycomb = poses_3d_df_honeycomb.to_dict(orient='records')
    client = honeycomb_io.core.generate_client(
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    logger.info('Writing 3D pose data')
    result = client.bulk_mutation(
        request_name='createPose3D',
        arguments={
            'pose3D': {
                'type': 'Pose3DInput',
                'value': poses_3d_list_honeycomb
            }
        },
        return_object=[
            'pose_id'
        ],
        chunk_size=chunk_size
    )
    try:
        pose_3d_ids = [datum['pose_id'] for datum in result]
    except:
        raise ValueError('Received unexpected result from Honeycomb:\n{}'.format(result))
    return pose_3d_ids

# Not currently used
def delete_3d_pose_data_by_inference_id(
    inference_id,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    pose_ids = fetch_pose_3d_ids(
        inference_id,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    statuses = delete_3d_pose_data_by_pose_ids(
        pose_ids,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    return pose_ids

# Not currently used
def fetch_pose_3d_ids(
    inference_id,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    query_list=[{
        'field': 'source',
        'operator': 'EQ',
        'value': inference_id
    }]
    return_data=['pose_id']
    result = search_3d_poses(
        query_list=query_list,
        return_data=return_data,
        chunk_size=chunk_size,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    pose_ids = [datum.get('pose_id') for datum in result]
    return pose_ids

# Not currently used
def delete_3d_pose_data_by_pose_ids(
    pose_ids,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if len(pose_ids) == 0:
        return pose_ids
    client = honeycomb_io.core.generate_client(
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    result = client.bulk_mutation(
        request_name='deletePose3D',
        arguments={
            'pose_id': {
                'type': 'ID',
                'value': pose_ids
            }
        },
        return_object=['status'],
        chunk_size=chunk_size
    )
    statuses = [datum.get('status') for datum in result]
    return statuses

# Not currently used
def write_pose_tracks_3d(
    poses_3d_df,
    source_id,
    source_type,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    poses_3d_df_copy = poses_3d_df.copy()
    current_index_name = poses_3d_df_copy.index.name
    poses_3d_df_copy = poses_3d_df_copy.reset_index().rename(columns={current_index_name: 'pose_3d_id'})
    pose_tracks_3d_df = poses_3d_df_copy.groupby('pose_track_3d_id').agg(
        poses_3d = pd.NamedAgg(
            column='pose_3d_id',
            aggfunc = lambda x: x.tolist()
        )
    )
    pose_tracks_3d_df['source'] = source_id
    pose_tracks_3d_df['source_type'] = source_type
    pose_tracks_3d_list = pose_tracks_3d_df.to_dict(orient='records')
    client = honeycomb_io.core.generate_client(
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    logger.info('Writing 3D pose tracks')
    result = client.bulk_mutation(
        request_name='createPoseTrack3D',
        arguments={
            'poseTrack3D': {
                'type': 'PoseTrack3DInput',
                'value': pose_tracks_3d_list
            }
        },
        return_object=[
            'pose_track_id'
        ],
        chunk_size=chunk_size
    )
    try:
        pose_track_3d_ids = [datum['pose_track_id'] for datum in result]
    except:
        raise ValueError('Received unexpected result from Honeycomb:\n{}'.format(result))
    return pose_track_3d_ids
