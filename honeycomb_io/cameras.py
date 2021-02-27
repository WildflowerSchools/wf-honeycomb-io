import honeycomb_io.core
import minimal_honeycomb
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

DEFAULT_CAMERA_DEVICE_TYPES = [
    'PI3WITHCAMERA',
    'PI4WITHCAMERA',
    'PIZEROWITHCAMERA'
]

def fetch_camera_ids_from_environment(
    start=None,
    end=None,
    environment_id=None,
    environment_name=None,
    camera_device_types=None,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if camera_device_types is None:
        camera_device_types = DEFAULT_CAMERA_DEVICE_TYPES
    environment_id = honeycomb_io.environments.fetch_environment_id(
        environment_id=environment_id,
        environment_name=environment_name,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    if environment_id is None:
        return None
    logger.info('Fetching camera assignments for specified environment and time span')
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
        request_name='getEnvironment',
        arguments={
            'environment_id': {
                'type': 'ID!',
                'value': environment_id
            }
        },
        return_object=[
            {'assignments': [
                'start',
                'end',
                {'assigned': [
                    {'... on Device': [
                        'device_id',
                        'device_type'
                    ]}
                ]}
            ]}
        ]
    )
    filtered_assignments = minimal_honeycomb.filter_assignments(
        assignments=result.get('assignments'),
        start_time=start,
        end_time=end
    )
    camera_device_ids = list()
    for assignment in filtered_assignments:
        device_type = assignment.get('assigned').get('device_type')
        if device_type is not None and device_type in camera_device_types:
            camera_device_ids.append(assignment.get('assigned').get('device_id'))
    if len(camera_device_ids) == 0:
        raise ValueError('No camera devices found in specified environment for specified time span')
    logger.info('Found {} camera assignments for specified environment and time span'.format(len(camera_device_ids)))
    return camera_device_ids

def fetch_camera_assignment_ids_from_environment(
    start=None,
    end=None,
    environment_id=None,
    environment_name=None,
    camera_device_types=None,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if camera_device_types is None:
        camera_device_types = DEFAULT_CAMERA_DEVICE_TYPES
    environment_id = honeycomb_io.environments.fetch_environment_id(
        environment_id=environment_id,
        environment_name=environment_name,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    if environment_id is None:
        return None
    logger.info('Fetching camera assignments for specified environment and time span')
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
        request_name='getEnvironment',
        arguments={
            'environment_id': {
                'type': 'ID!',
                'value': environment_id
            }
        },
        return_object=[
            {'assignments': [
                'assignment_id',
                'start',
                'end',
                {'assigned': [
                    {'... on Device': [
                        'device_id',
                        'device_type'
                    ]}
                ]}
            ]}
        ]
    )
    filtered_assignments = minimal_honeycomb.filter_assignments(
        assignments=result.get('assignments'),
        start_time=start,
        end_time=end
    )
    camera_assignment_ids = list()
    for assignment in filtered_assignments:
        device_type = assignment.get('assigned').get('device_type')
        if device_type is not None and device_type in camera_device_types:
            camera_assignment_ids.append(assignment.get('assignment_id'))
    if len(camera_assignment_ids) == 0:
        raise ValueError('No camera devices found in specified environment for specified time span')
    logger.info('Found {} camera assignments for specified environment and time span'.format(len(camera_assignment_ids)))
    return camera_assignment_ids

def fetch_camera_ids_from_camera_properties(
    camera_ids=None,
    camera_device_types=None,
    camera_part_numbers=None,
    camera_names=None,
    camera_serial_numbers=None,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if camera_ids is not None:
        if camera_names is not None or camera_part_numbers is not None or camera_serial_numbers is not None:
            raise ValueError('If camera IDs are specified, camera names/part numbers/serial numbers cannot be specified')
        return camera_ids
    if camera_names is not None or camera_part_numbers is not None or camera_serial_numbers is not None:
        query_list=list()
        if camera_device_types is not None:
            query_list.append({
                'field': 'device_type',
                'operator': 'IN',
                'values': camera_device_types
            })
        if camera_part_numbers is not None:
            query_list.append({
                'field': 'part_number',
                'operator': 'IN',
                'values': camera_part_numbers
            })
        if camera_names is not None:
            query_list.append({
                'field': 'name',
                'operator': 'IN',
                'values': camera_names
            })
        if camera_serial_numbers is not None:
            query_list.append({
                'field': 'serial_number',
                'operator': 'IN',
                'values': camera_serial_numbers
            })
        logger.info('Fetching camera IDs for cameras with specified properties')
        client = honeycomb_io.core.generate_client(
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
        result = client.bulk_query(
            request_name='searchDevices',
            arguments={
                'query': {
                    'type': 'QueryExpression!',
                    'value': {
                        'operator': 'AND',
                        'children': query_list
                    }
                }
            },
            return_data=[
                'device_id'
            ],
            id_field_name='device_id'
        )
        if len(result) == 0:
            raise ValueError('No devices match specified device types/part numbers/names/serial numbers')
        camera_ids = [datum.get('device_id') for datum in result]
        logger.info('Found {} camera IDs that match specified properties'.format(len(camera_ids)))
        return camera_ids
    return None

def fetch_camera_names(
    camera_ids,
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
    logger.info('Fetching camera names for specified camera device IDs')
    result = client.bulk_query(
        request_name='searchDevices',
        arguments={
            'query': {
                'type': 'QueryExpression!',
                'value': {
                    'field': 'device_id',
                    'operator': 'IN',
                    'values': camera_ids
                }
            }
        },
        return_data=[
            'device_id',
            'name'
        ],
        id_field_name = 'device_id',
        chunk_size=chunk_size
    )
    camera_names = {device.get('device_id'): device.get('name') for device in result}
    logger.info('Fetched {} camera names'.format(len(camera_names)))
    return camera_names

def fetch_camera_calibrations(
    camera_ids,
    start=None,
    end=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    intrinsic_calibrations = fetch_intrinsic_calibrations(
        camera_ids=camera_ids,
        start=start,
        end=end,
        chunk_size=chunk_size,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    extrinsic_calibrations = fetch_extrinsic_calibrations(
        camera_ids=camera_ids,
        start=start,
        end=end,
        chunk_size=chunk_size,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
    )
    camera_calibrations = dict()
    for camera_id in camera_ids:
        if camera_id not in intrinsic_calibrations.keys():
            logger.warning('No intrinsic calibration found for camera ID {}'.format(
                camera_id
            ))
            continue
        if camera_id not in extrinsic_calibrations.keys():
            logger.warning('No extrinsic calibration found for camera ID {}'.format(
                camera_id
            ))
            continue
        camera_calibrations[camera_id] = {**intrinsic_calibrations[camera_id], **extrinsic_calibrations[camera_id]}
    return camera_calibrations

def fetch_intrinsic_calibrations(
    camera_ids,
    start=None,
    end=None,
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
    logger.info('Fetching intrinsic calibrations for specified camera device IDs and time span')
    result = client.bulk_query(
        request_name='searchIntrinsicCalibrations',
        arguments={
            'query': {
                'type': 'QueryExpression!',
                'value': {
                    'field': 'device',
                    'operator': 'IN',
                    'values': camera_ids
                }
            }
        },
        return_data=[
            'intrinsic_calibration_id',
            'start',
            'end',
            {'device': [
                'device_id'
            ]},
            'camera_matrix',
            'distortion_coefficients',
            'image_width',
            'image_height'
        ],
        id_field_name = 'intrinsic_calibration_id',
        chunk_size=chunk_size
    )
    logger.info('Fetched {} intrinsic calibrations for specified camera IDs'.format(len(result)))
    filtered_result = minimal_honeycomb.filter_assignments(
        result,
        start,
        end
    )
    logger.info('{} intrinsic calibrations are consistent with specified start and end times'.format(len(filtered_result)))
    intrinsic_calibrations = dict()
    for datum in filtered_result:
        camera_id = datum.get('device').get('device_id')
        if camera_id in intrinsic_calibrations.keys():
            raise ValueError('More than one intrinsic calibration found for camera {}'.format(
                camera_id
            ))
        intrinsic_calibrations[camera_id] = {
            'camera_matrix': np.asarray(datum.get('camera_matrix')),
            'distortion_coefficients': np.asarray(datum.get('distortion_coefficients')),
            'image_width': datum.get('image_width'),
            'image_height': datum.get('image_height')
        }
    return intrinsic_calibrations

def fetch_extrinsic_calibrations(
    camera_ids,
    start=None,
    end=None,
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
    logger.info('Fetching extrinsic calibrations for specified camera device IDs and time span')
    result = client.bulk_query(
        request_name='searchExtrinsicCalibrations',
        arguments={
            'query': {
                'type': 'QueryExpression!',
                'value': {
                    'field': 'device',
                    'operator': 'IN',
                    'values': camera_ids
                }
            }
        },
        return_data=[
            'extrinsic_calibration_id',
            'start',
            'end',
            {'device': [
                'device_id'
            ]},
            {'coordinate_space': [
                'space_id'
            ]},
            'translation_vector',
            'rotation_vector'
        ],
        id_field_name = 'extrinsic_calibration_id',
        chunk_size=chunk_size
    )
    logger.info('Fetched {} extrinsic calibrations for specified camera IDs'.format(len(result)))
    filtered_result = minimal_honeycomb.filter_assignments(
        result,
        start,
        end
    )
    logger.info('{} extrinsic calibrations are consistent with specified start and end times'.format(len(filtered_result)))
    extrinsic_calibrations = dict()
    space_ids = list()
    for datum in filtered_result:
        camera_id = datum.get('device').get('device_id')
        space_id = datum.get('coordinate_space').get('space_id')
        space_ids.append(space_id)
        if camera_id in extrinsic_calibrations.keys():
            raise ValueError('More than one extrinsic calibration found for camera {}'.format(
                camera_id
            ))
        extrinsic_calibrations[camera_id] = {
            'space_id': space_id,
            'rotation_vector': np.asarray(datum.get('rotation_vector')),
            'translation_vector': np.asarray(datum.get('translation_vector'))
        }
    if len(np.unique(space_ids)) > 1:
        raise ValueError('More than one coordinate space found among fetched calibrations')
    return extrinsic_calibrations

def fetch_camera_device_id_lookup(
    assignment_ids,
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
        request_name='searchAssignments',
        arguments={
            'query': {
                'type': 'QueryExpression!',
                'value': {
                    'field': 'assignment_id',
                    'operator': 'IN',
                    'values': assignment_ids
                }
        }},
        return_data=[
            'assignment_id',
            {'assigned': [
                {'... on Device': [
                    'device_id'
                ]}
            ]}
        ],
        id_field_name='assignment_id'
    )
    camera_device_id_lookup = dict()
    for datum in result:
        camera_device_id_lookup[datum.get('assignment_id')] = datum.get('assigned').get('device_id')
    return camera_device_id_lookup

def fetch_camera_info(
    environment_name,
    start_time,
    end_time,
    camera_device_types=DEFAULT_CAMERA_DEVICE_TYPES
):
    logger.info('Fetching camera info between start time {} and end time {} for environment {}'.format(
        start_time.isoformat(),
        end_time.isoformat(),
        environment_name
    ))
    device_ids = fetch_camera_device_ids(
        environment_name=environment_name,
        start_time=start_time,
        end_time=end_time,
        camera_device_types=camera_device_types
    )
    logger.info('Fetching camera info between start time {} and end time {} for the following device_ids: {}'.format(
        start_time.isoformat(),
        end_time.isoformat(),
        device_ids
    ))
    client = minimal_honeycomb.MinimalHoneycombClient()
    result = client.request(
        request_type='query',
        request_name='searchDevices',
        arguments={
            'query': {
                'type': 'QueryExpression!',
                'value': {
                    'field': 'device_id',
                    'operator': 'IN',
                    'values': device_ids
                }
            }
        },
        return_object=[
            {'data': [
                'device_id',
                'name',
                {'intrinsic_calibrations': [
                    'start',
                    'end',
                    'camera_matrix',
                    'distortion_coefficients',
                    'image_width',
                    'image_height'
                ]},
                {'extrinsic_calibrations': [
                    'start',
                    'end',
                    'translation_vector',
                    'rotation_vector'
                ]}
            ]}
        ]
    )
    devices = result.get('data')
    camera_info_dict = dict()
    for device in devices:
        intrinsic_calibration = extract_assignment(
            assignments=device['intrinsic_calibrations'],
            start_time=start_time,
            end_time=end_time
        )
        if intrinsic_calibration is None:
            continue
        extrinsic_calibration = extract_assignment(
            assignments=device['extrinsic_calibrations'],
            start_time=start_time,
            end_time=end_time
        )
        if extrinsic_calibration is None:
            continue
        camera_info_dict[device['device_id']] = {
            'device_name': device['name'],
            'camera_matrix': intrinsic_calibration['camera_matrix'],
            'distortion_coefficients': intrinsic_calibration['distortion_coefficients'],
            'image_width': intrinsic_calibration['image_width'],
            'image_height': intrinsic_calibration['image_height'],
            'translation_vector': extrinsic_calibration['translation_vector'],
            'rotation_vector': extrinsic_calibration['rotation_vector'],
        }
    return camera_info_dict


def fetch_camera_device_ids(
    environment_name,
    start_time,
    end_time,
    camera_device_types=DEFAULT_CAMERA_DEVICE_TYPES
):
    client = minimal_honeycomb.MinimalHoneycombClient()
    result = client.request(
        request_type='query',
        request_name='findEnvironments',
        arguments={
            'name': {
                'type': 'String',
                'value': environment_name
            }
        },
        return_object=[
            {'data': [
                {'assignments': [
                    'start',
                    'end',
                    'assigned_type',
                    {'assigned': [
                        '__typename',
                        {'... on Device': [
                            'device_id',
                            'device_type'
                        ]}
                    ]}
                ]}
            ]}
        ]
    )
    environments = result.get('data')
    if len(environments) == 0:
        raise ValueError(
            'No environments match environment name {}'.format(environment_name))
    if len(environments) > 1:
        raise ValueError(
            'More than one environments matched name {}'.format(environment_name))
    assignments = environments[0].get('assignments')
    camera_device_ids = list()
    for assignment in assignments:
        if assignment.get('start') is not None and (pd.to_datetime(
                assignment.get('start')).to_pydatetime() > end_time):
            continue
        if assignment.get('end') is not None and (pd.to_datetime(
                assignment.get('end')).to_pydatetime() < start_time):
            continue
        if assignment.get('assigned').get('__typename') != 'Device':
            continue
        if assignment.get('assigned').get(
                'device_type') not in camera_device_types:
            continue
        camera_device_ids.append(assignment.get('assigned').get('device_id'))
    return camera_device_ids

def extract_assignment(
    assignments,
    start_time,
    end_time
):
    matched_assignments = list()
    for assignment in assignments:
        if assignment.get('start') is not None and (pd.to_datetime(
                assignment.get('start')).to_pydatetime() > end_time):
            continue
        if assignment.get('end') is not None and (pd.to_datetime(
                assignment.get('end')).to_pydatetime() < start_time):
            continue
        matched_assignments.append(assignment)
    if len(matched_assignments) == 0:
        return None
    if len(matched_assignments) > 1:
        raise ValueError('Multiple assignments matched start time {} and end time {}'.format(
            start_time.isoformat(),
            end_time.isoformat()
        ))
    return matched_assignments[0]