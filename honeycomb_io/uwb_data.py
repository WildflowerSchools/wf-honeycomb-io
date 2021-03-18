import honeycomb_io.core
import honeycomb_io.utils
import honeycomb_io.environments
import honeycomb_io.devices
import honeycomb_io.materials
import honeycomb_io.exceptions
import minimal_honeycomb
import pandas as pd
import numpy as np
import datetime
import json
import logging

# from process_cuwb_data.utils.log import logger

logger = logging.getLogger(__name__)

# CUWB Data Protocol (https://cuwb.io/docs/v3.1/software-integration/cdp-output-definition/)
POSITION_SCALE_FACTOR = 1000.0 # Convert millimeters to meters
ACCELEROMETER_BYTE_SIZE = 4
GYROSCOPE_BYTE_SIZE = 4
MAGNETOMETER_BYTE_SIZE = 4
CUWB_DATA_MAX_INT = {
    1: 127,
    2: 32767,
    4: 2147483647
}

SUPPORTED_CUWB_DATA_TYPES = ['position', 'accelerometer', 'gyroscope', 'magnetometer']

OBJECT_NAMES = {
    'position': 'Position',
    'accelerometer': 'AccelerometerData',
    'gyroscope': 'GyroscopeData',
    'magnetometer': 'MagnetometerData'
}

def raw_cuwb_data_lists_to_parsed(
    raw_data_lists,
    device_types=['UWBTAG'],
    coordinate_space_id=None,
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    data_id_lists = dict()
    for data_type in SUPPORTED_CUWB_DATA_TYPES:
        if data_type in raw_data_lists.keys():
            if data_type not in SUPPORTED_CUWB_DATA_TYPES:
                raise ValueError('Data type must be one of {}'.format(
                    SUPPORTED_CUWB_DATA_TYPES
                ))
            data_id_lists[data_type] = raw_cuwb_data_to_parsed(
                raw_data_lists[data_type],
                data_type,
                device_types=device_types,
                coordinate_space_id=coordinate_space_id,
                chunk_size=chunk_size,
                client=client,
                uri=uri,
                token_uri=token_uri,
                audience=audience,
                client_id=client_id,
                client_secret=client_secret
            )
    return data_id_lists

def raw_cuwb_data_to_parsed(
        raw_data,
        data_type,
        device_types=['UWBTAG'],
        coordinate_space_id=None,
        chunk_size=1000,
        client=None,
        uri=None,
        token_uri=None,
        audience=None,
        client_id=None,
        client_secret=None
):
    if data_type not in SUPPORTED_CUWB_DATA_TYPES:
        raise ValueError('Data type must be one of {}'.format(
            SUPPORTED_CUWB_DATA_TYPES
        ))
    num_raw_observations = len(raw_data)
    if num_raw_observations == 0:
        logger.warn('List of raw CUWB {} observations is empty'.format(
            data_type
        ))
        return []
    logger.info('Processing {} raw CUWB {} observations'.format(
        num_raw_observations,
        data_type
    ))
    serial_numbers = extract_serial_numbers(
        raw_data=raw_data
    )
    num_serial_numbers = len(serial_numbers)
    if num_serial_numbers == 0:
        logger.warn('Raw CUWB {} observations appear to contain no serial numbers'.format(
            device_types
        ))
        return []
    device_id_lookup = honeycomb_io.fetch_uwb_device_id_lookup(
        serial_numbers=serial_numbers,
        device_types=device_types,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    num_uwb_devices = len(device_id_lookup)
    if num_uwb_devices == 0:
        logger.warn('Extracted serial numbers ({}) appear to contain no UWB devices corresponding to target device types ({})'.format(
            serial_numbers,
            device_types
        ))
        return []
    parsed_data = parse_raw_cuwb_data(
        raw_data=raw_data,
        data_type=data_type,
        device_id_lookup=device_id_lookup,
        coordinate_space_id=coordinate_space_id,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    num_parsed_observations = len(parsed_data)
    if num_parsed_observations == 0:
        logger.warn('Raw CUWB observations appear to contain no data for target device types ({})'.format(
            device_types
        ))
        return []
    return parsed_data

def write_raw_cuwb_data_lists(
    raw_data_lists,
    device_types=['UWBTAG'],
    coordinate_space_id=None,
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    data_id_lists = dict()
    for data_type in SUPPORTED_CUWB_DATA_TYPES:
        if data_type in raw_data_lists.keys():
            try:
                data_ids = write_raw_cuwb_data(
                    raw_data=raw_data_lists[data_type],
                    data_type=data_type,
                    device_types=device_types,
                    coordinate_space_id=coordinate_space_id,
                    chunk_size=chunk_size,
                    client=client,
                    uri=uri,
                    token_uri=token_uri,
                    audience=audience,
                    client_id=client_id,
                    client_secret=client_secret
                )
            except(honeycomb_io.exceptions.HoneycombWriteError):
                logger.warn('Error occurred during write. Attempting to roll back changes')
                if len(data_id_lists) > 0:
                    try:
                        delete_cuwb_data(
                            data_id_lists=data_id_lists,
                            chunk_size=chunk_size,
                            client=client,
                            uri=uri,
                            token_uri=token_uri,
                            audience=audience,
                            client_id=client_id,
                            client_secret=client_secret
                        )
                    except(honeycomb_io.exceptions.HoneycombDeleteError):
                        raise honeycomb_io.exceptions.HoneycombWriteErrorNoRetryCleanupFailed(
                            'Write failed and attempt to roll back changes failed'
                        )
                raise
            data_id_lists[data_type] = data_ids
    return data_id_lists

def write_raw_cuwb_data(
    raw_data,
    data_type,
    device_types=['UWBTAG'],
    coordinate_space_id=None,
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    parsed_data = raw_cuwb_data_to_parsed(
        raw_data,
        data_type,
        device_types=device_types,
        coordinate_space_id=coordinate_space_id,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    data_ids = write_cuwb_data(
        parsed_data=parsed_data,
        data_type=data_type,
        chunk_size=chunk_size,
        client=client,
        uri=uri,
        token_uri=token_uri,
        audience=audience,
        client_id=client_id,
        client_secret=client_secret
    )
    num_uploaded_observations = len(data_ids)
    if num_uploaded_observations == 0:
        logger.warn('Honeycomb reports that no data was written')
    num_raw_observations = len(raw_data)
    logger.info('Uploaded {} observations from the {} supplied raw CUWB {} observations'.format(
        num_uploaded_observations,
        num_raw_observations,
        data_type
    ))
    return data_ids

def write_cuwb_data(
    parsed_data,
    data_type,
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if data_type == 'position':
        data_ids = write_position_data(
            position_data=parsed_data,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
        return data_ids
    elif data_type == 'accelerometer':
        data_ids = write_accelerometer_data(
            accelerometer_data=parsed_data,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
        return data_ids
    elif data_type == 'gyroscope':
        data_ids = write_gyroscope_data(
            gyroscope_data=parsed_data,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
        return data_ids
    elif data_type == 'magnetometer':
        data_ids = write_magnetometer_data(
            magnetometer_data=parsed_data,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
        return data_ids
    else:
        raise ValueError('Data type {} not currently supported'.format(
            data_type
        ))

def write_position_data(
    position_data,
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    num_parsed_observations = len(position_data)
    logger.info('Writing {} CUWB position observations to Honeycomb'.format(
        num_parsed_observations
    ))
    if num_parsed_observations == 0:
        logger.warn('List of CUWB position observations is empty')
        return []
    try:
        position_data_ids = honeycomb_io.core.create_objects(
            object_name='Position',
            data=position_data,
            request_name=None,
            argument_name=None,
            argument_type=None,
            id_field_name=None,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    except:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Encountered problem when attempting to write assignment data'
        )
    logger.info('Successfully wrote {} CUWB position observations to Honeycomb'.format(
        len(position_data_ids)
    ))
    return position_data_ids

def write_accelerometer_data(
    accelerometer_data,
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    num_parsed_observations = len(accelerometer_data)
    logger.info('Writing {} CUWB accelerometer observations to Honeycomb'.format(
        num_parsed_observations
    ))
    if num_parsed_observations == 0:
        logger.warn('List of CUWB accelerometer observations is empty')
        return []
    try:
        accelerometer_data_ids = honeycomb_io.core.create_objects(
            object_name='AccelerometerData',
            data=accelerometer_data,
            request_name=None,
            argument_name=None,
            argument_type=None,
            id_field_name=None,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    except:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Encountered problem when attempting to write accelerometer data'
        )
    logger.info('Successfully wrote {} CUWB accelerometer observations to Honeycomb'.format(
        len(accelerometer_data_ids)
    ))
    return accelerometer_data_ids

def write_gyroscope_data(
    gyroscope_data,
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    num_parsed_observations = len(gyroscope_data)
    logger.info('Writing {} CUWB gyroscope observations to Honeycomb'.format(
        num_parsed_observations
    ))
    if num_parsed_observations == 0:
        logger.warn('List of CUWB gyroscope observations is empty')
        return []
    try:
        gyroscope_data_ids = honeycomb_io.core.create_objects(
            object_name='GyroscopeData',
            data=gyroscope_data,
            request_name=None,
            argument_name=None,
            argument_type=None,
            id_field_name=None,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    except:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Encountered problem when attempting to write gyroscope data'
        )
    logger.info('Successfully wrote {} CUWB gyroscope observations to Honeycomb'.format(
        len(gyroscope_data_ids)
    ))
    return gyroscope_data_ids

def write_magnetometer_data(
    magnetometer_data,
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    num_parsed_observations = len(magnetometer_data)
    logger.info('Writing {} CUWB magnetometer observations to Honeycomb'.format(
        num_parsed_observations
    ))
    if num_parsed_observations == 0:
        logger.warn('List of CUWB magnetometer observations is empty')
        return []
    try:
        magnetometer_data_ids = honeycomb_io.core.create_objects(
            object_name='MagnetometerData',
            data=magnetometer_data,
            request_name=None,
            argument_name=None,
            argument_type=None,
            id_field_name=None,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    except:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Encountered problem when attempting to write magnetometer data'
        )
    logger.info('Successfully wrote {} CUWB magnetometer observations to Honeycomb'.format(
        len(magnetometer_data_ids)
    ))
    return magnetometer_data_ids

def delete_cuwb_data(
    data_id_lists,
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    for data_type in data_id_lists.keys():
        ids = data_id_lists[data_type]
        object_name = OBJECT_NAMES[data_type]
        logger.info('Attempting to delete {} {} observations'.format(
            len(ids),
            data_type
        ))
        try:
            statuses = honeycomb_io.core.delete_objects(
                object_name=object_name,
                ids=ids,
                request_name=None,
                id_field_name=None,
                chunk_size=chunk_size,
                client=client,
                uri=uri,
                token_uri=token_uri,
                audience=audience,
                client_id=client_id,
                client_secret=client_secret
            )
        except:
            raise honeycomb_io.exceptions.HoneycombDeleteError(
                'Encountered problem when attempting to delete {} data'.format(
                    data_type
                )
            )
        if len(statuses) != len(ids):
            raise honeycomb_io.exceptions.HoneycombDeleteError(
                'Returned status vector different length than id vector when attempting to delete {} data'.format(
                    data_type
                )
            )
        if set([item['status'] for item in statuses]) != {'ok'}:
            raise honeycomb_io.exceptions.HoneycombDeleteError(
                'Generated errors when attempting to delete {} data'.format(
                    data_type
                )
            )

def parse_raw_cuwb_data(
    raw_data,
    data_type,
    device_id_lookup,
    coordinate_space_id=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if data_type=='position':
        parsed_data = parse_raw_position_data(
            raw_position_data=raw_data,
            device_id_lookup=device_id_lookup,
            coordinate_space_id=coordinate_space_id,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
        return parsed_data
    elif data_type=='accelerometer':
        parsed_data = parse_raw_accelerometer_data(
            raw_accelerometer_data=raw_data,
            device_id_lookup=device_id_lookup
        )
        return parsed_data
    elif data_type=='gyroscope':
        parsed_data = parse_raw_gyroscope_data(
            raw_gyroscope_data=raw_data,
            device_id_lookup=device_id_lookup
        )
        return parsed_data
    elif data_type=='magnetometer':
        parsed_data = parse_raw_magnetometer_data(
            raw_magnetometer_data=raw_data,
            device_id_lookup=device_id_lookup
        )
        return parsed_data
    else:
        raise ValueError('Data type {} not currently supported'.format(
            data_type
        ))

def parse_raw_position_data(
    raw_position_data,
    device_id_lookup,
    coordinate_space_id=None,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    num_raw_observations = len(raw_position_data)
    num_tag_ids = len(device_id_lookup)
    logger.info('Parsing {} raw CUWB position observations looking for {} serial numbers: {}'.format(
        num_raw_observations,
        num_tag_ids,
        list(device_id_lookup.keys())
    ))
    if num_raw_observations == 0:
        logger.warn('List of raw CUWB position observations is empty')
        return []
    if coordinate_space_id is None:
        try:
            timestamps = pd.to_datetime([datum.get('timestamp') for datum in raw_position_data])
            device_ids = list(set([
                device_id_lookup[datum.get('serial_number')]
                for datum in raw_position_data
                if datum.get('serial_number') in device_id_lookup.keys()
            ]))
        except:
            raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
                'Failed to extract timestamps and serial numbers from position data'
            )
        coordinate_space_id = fetch_coordinate_space_id(
            device_ids=device_ids,
            start=timestamps.min().to_pydatetime(),
            end=timestamps.max().to_pydatetime(),
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    position_data = list()
    try:
        for datum in raw_position_data:
            if datum['serial_number'] in device_id_lookup.keys():
                position_data.append({
                    'timestamp': honeycomb_io.utils.to_honeycomb_datetime(datum['timestamp']),
                    'coordinate_space': coordinate_space_id,
                    'object': device_id_lookup[datum['serial_number']],
                    'coordinates': [
                        datum['x']/POSITION_SCALE_FACTOR,
                        datum['y']/POSITION_SCALE_FACTOR,
                        datum['z']/POSITION_SCALE_FACTOR
                    ],
                    'source_type': 'MEASURED'
                })
    except:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Failed to parse position data'
        )
    num_parsed_observations = len(position_data)
    logger.info('Data yielded {} CUWB position observations for target serial numbers ({})'.format(
        num_parsed_observations,
        list(device_id_lookup.keys())
    ))
    return position_data

def parse_raw_accelerometer_data(
    raw_accelerometer_data,
    device_id_lookup
):
    num_raw_observations = len(raw_accelerometer_data)
    num_tag_ids = len(device_id_lookup)
    logger.info('Parsing {} raw CUWB accelerometer observations looking for {} serial numbers: {}'.format(
        num_raw_observations,
        num_tag_ids,
        list(device_id_lookup.keys())
    ))
    if num_raw_observations == 0:
        logger.warn('List of raw CUWB accelerometer observations is empty')
        return []
    accelerometer_data = list()
    try:
        for datum in raw_accelerometer_data:
            if datum['serial_number'] in device_id_lookup.keys():
                accelerometer_data.append({
                    'timestamp': honeycomb_io.utils.to_honeycomb_datetime(datum['timestamp']),
                    'device': device_id_lookup[datum['serial_number']],
                    'data': [
                        datum['x']*datum['scale']/CUWB_DATA_MAX_INT[ACCELEROMETER_BYTE_SIZE],
                        datum['y']*datum['scale']/CUWB_DATA_MAX_INT[ACCELEROMETER_BYTE_SIZE],
                        datum['z']*datum['scale']/CUWB_DATA_MAX_INT[ACCELEROMETER_BYTE_SIZE]
                    ]
                })
    except:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Failed to parse accelerometer data'
        )
    num_parsed_observations = len(accelerometer_data)
    logger.info('Data yielded {} CUWB accelerometer observations for target serial numbers ({})'.format(
        num_parsed_observations,
        list(device_id_lookup.keys())
    ))
    return accelerometer_data

def parse_raw_gyroscope_data(
    raw_gyroscope_data,
    device_id_lookup
):
    num_raw_observations = len(raw_gyroscope_data)
    num_tag_ids = len(device_id_lookup)
    logger.info('Parsing {} raw CUWB gyroscope observations looking for {} serial numbers: {}'.format(
        num_raw_observations,
        num_tag_ids,
        list(device_id_lookup.keys())
    ))
    if num_raw_observations == 0:
        logger.warn('List of raw CUWB gyroscope observations is empty')
        return []
    gyroscope_data = list()
    try:
        for datum in raw_gyroscope_data:
            if datum['serial_number'] in device_id_lookup.keys():
                gyroscope_data.append({
                    'timestamp': honeycomb_io.utils.to_honeycomb_datetime(datum['timestamp']),
                    'device': device_id_lookup[datum['serial_number']],
                    'data': [
                        datum['x']*datum['scale']/CUWB_DATA_MAX_INT[GYROSCOPE_BYTE_SIZE],
                        datum['y']*datum['scale']/CUWB_DATA_MAX_INT[GYROSCOPE_BYTE_SIZE],
                        datum['z']*datum['scale']/CUWB_DATA_MAX_INT[GYROSCOPE_BYTE_SIZE]
                    ]
                })
    except:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Failed to parse gyroscope data'
        )
    num_parsed_observations = len(gyroscope_data)
    logger.info('Data yielded {} CUWB gyroscope observations for target serial numbers ({})'.format(
        num_parsed_observations,
        list(device_id_lookup.keys())
    ))
    return gyroscope_data

def parse_raw_magnetometer_data(
    raw_magnetometer_data,
    device_id_lookup
):
    num_raw_observations = len(raw_magnetometer_data)
    num_tag_ids = len(device_id_lookup)
    logger.info('Parsing {} raw CUWB magnetometer observations looking for {} serial numbers: {}'.format(
        num_raw_observations,
        num_tag_ids,
        list(device_id_lookup.keys())
    ))
    if num_raw_observations == 0:
        logger.warn('List of raw CUWB magnetometer observations is empty')
        return []
    magnetometer_data = list()
    try:
        for datum in raw_magnetometer_data:
            if datum['serial_number'] in device_id_lookup.keys():
                magnetometer_data.append({
                    'timestamp': honeycomb_io.utils.to_honeycomb_datetime(datum['timestamp']),
                    'device': device_id_lookup[datum['serial_number']],
                    'data': [
                        datum['x']*datum['scale']/CUWB_DATA_MAX_INT[MAGNETOMETER_BYTE_SIZE],
                        datum['y']*datum['scale']/CUWB_DATA_MAX_INT[MAGNETOMETER_BYTE_SIZE],
                        datum['z']*datum['scale']/CUWB_DATA_MAX_INT[MAGNETOMETER_BYTE_SIZE]
                    ]
                })
    except:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Failed to parse magnetometer data'
        )
    num_parsed_observations = len(magnetometer_data)
    logger.info('Data yielded {} CUWB magnetometer observations for target serial numbers ({})'.format(
        num_parsed_observations,
        list(device_id_lookup.keys())
    ))
    return magnetometer_data

def extract_serial_numbers(
    raw_data
):
    num_raw_observations = len(raw_data)
    logger.info('Extracting serial numbers from {} raw CUWB data observations'.format(
        num_raw_observations
    ))
    if num_raw_observations == 0:
        logger.warn('List of raw CUWB accelerometer observations is empty')
        return []
    serial_numbers = set()
    for datum in raw_data:
        serial_number = datum.get('serial_number')
        if serial_number is None:
            raise honeycomb_io.exceptions.HoneycombWriteErrorRetry('CUWB observation {} does not contain a serial number'.format(
                datum
            ))
        else:
            serial_numbers.add(serial_number)
    serial_numbers = list(serial_numbers)
    logger.info('Extracted {} serial numbers: {}'.format(
        len(serial_numbers),
        serial_numbers
    ))
    return serial_numbers

def fetch_uwb_device_id_lookup(
    serial_numbers,
    device_types=['UWBTAG'],
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    num_serial_numbers = len(serial_numbers)
    if num_serial_numbers == 0:
        logger.warn('List of serial numbers is empty')
        return {}
    logger.info('Fetching CUWB device ID info for target device types ({}) and specified serial_numbers ({})'.format(
        device_types,
        serial_numbers
    ))
    query_list = [
        {'field': 'device_type', 'operator': 'CONTAINED_BY', 'values': device_types},
        {'field': 'serial_number', 'operator': 'CONTAINED_BY', 'values': serial_numbers}
    ]
    return_data = [
        'device_id',
        'serial_number'
    ]
    try:
        result = honeycomb_io.core.search_objects(
            object_name='Device',
            query_list=query_list,
            return_data=return_data,
            request_name=None,
            id_field_name=None,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    except:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Encountered problem when attempting to fetch device info'
        )
    device_id_lookup = {datum['serial_number']: datum['device_id'] for datum in result}
    logger.info('Found {} UWB serial numbers ({}) corresponding to target types ({})'.format(
        len(device_id_lookup),
        list(device_id_lookup.keys()),
        device_types
    ))
    return device_id_lookup

def fetch_coordinate_space_id(
    device_ids,
    start,
    end,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    logger.info('Attempting to identify coordinate space id for device IDs {} for the period {} to {}'.format(
        device_ids,
        start.isoformat(),
        end.isoformat()
    ))
    logger.info('Attempting to identify environment for device IDs {} for the period {} to {}'.format(
        device_ids,
        start.isoformat(),
        end.isoformat()
    ))
    query_list = [
        {'field': 'assigned', 'operator': 'CONTAINED_BY', 'values': device_ids},
        {'field': 'start', 'operator': 'LTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(end)},
        {'operator': 'OR', 'children': [
            {'field': 'end', 'operator': 'ISNULL'},
            {'field': 'end', 'operator': 'GTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(start)}
        ]}
    ]
    return_data = [
        'assignment_id',
        {'environment': [
            'environment_id',
            'name'
        ]}
    ]
    try:
        result = honeycomb_io.core.search_objects(
            object_name='Assignment',
            query_list=query_list,
            return_data=return_data,
            request_name=None,
            id_field_name=None,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    except:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Encountered problem when attempting to fetch assignment data'
        )
    environment_ids = list(set([datum.get('environment', {}).get('environment_id') for datum in result]))
    environment_names = list(set([datum.get('environment', {}).get('name') for datum in result]))
    if len(environment_ids) == 0:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'No environments appear to be associated with specified devices in the specified period'
        )
    if len(environment_ids) > 1:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Multiple environments appear to be associated with specified devices in the specified period: {}'.format(
                environment_names
            )
        )
    environment_id = environment_ids[0]
    environment_name = environment_names[0]
    logger.info('Specified devices all appear to be assigned to the \'{}\' environment in the specified period'.format(
        environment_name
    ))
    logger.info('Attempting to identify coordinate space id for the \'{}\' environment for the period {} to {}'.format(
        environment_name,
        start.isoformat(),
        end.isoformat()
    ))
    query_list = [
        {'field': 'environment', 'operator': 'EQ', 'value': environment_id},
        {'field': 'start', 'operator': 'LTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(end)},
        {'operator': 'OR', 'children': [
            {'field': 'end', 'operator': 'ISNULL'},
            {'field': 'end', 'operator': 'GTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(start)}
        ]}
    ]
    return_data = [
        'space_id',
        'name'
    ]
    try:
        result = honeycomb_io.core.search_objects(
            object_name='CoordinateSpace',
            query_list=query_list,
            return_data=return_data,
            request_name=None,
            id_field_name=None,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    except:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Encountered problem when attempting to fetch coordinate space data'
        )
    coordinate_space_ids = list(set([datum.get('space_id') for datum in result]))
    coordinate_space_names = list(set([datum.get('name') for datum in result]))
    if len(coordinate_space_ids) == 0:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'No coordinate spaces appear to be associated with specified environment in the specified period'
        )
    if len(coordinate_space_ids) > 1:
        raise honeycomb_io.exceptions.HoneycombWriteErrorRetry(
            'Multiple coordinate spaces appear to be associated with specified environment in the specified period: {}'.format(
                coordinate_space_ids
            )
        )
    coordinate_space_id = coordinate_space_ids[0]
    coordinate_space_name = coordinate_space_names[0]
    logger.info('The \'{}\' environment appears to have a unique coordinate space defined in the specified period: \'{}\''.format(
        environment_name,
        coordinate_space_name
    ))
    return coordinate_space_id

def fetch_cuwb_position_data(
    start,
    end,
    device_ids=None,
    environment_id=None,
    environment_name=None,
    device_types=['UWBTAG'],
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if device_ids is None:
        device_ids = honeycomb_io.devices.fetch_device_ids(
            device_types=device_types,
            environment_id=environment_id,
            environment_name=environment_name,
            start=start,
            end=end,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    query_list = [
        {'field': 'timestamp', 'operator': 'GTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(start)},
        {'field': 'timestamp', 'operator': 'LTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(end)}
    ]
    if device_ids is not None:
        query_list.append(
            {'field': 'object', 'operator': 'CONTAINED_BY', 'values': device_ids}
        )
    return_data = [
        'position_id',
        'timestamp',
        {'coordinate_space': [
            'space_id'
        ]},
        {'object': [
            {'... on Device': [
                'device_id',
                'part_number',
                'serial_number',
                'tag_id',
                'name'
            ]}
        ]},
        'coordinates'
    ]
    if device_ids is not None:
        logger.info('Fetching position data for devices {} for period {} to {}'.format(
            device_ids,
            start.isoformat(),
            end.isoformat()
        ))
    else:
        logger.info('Fetching all position data for period {} to {}'.format(
            start.isoformat(),
            end.isoformat()
        ))
    results = honeycomb_io.core.search_objects(
        object_name='Position',
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
    logger.info('Fetched {} position observations'.format(
        len(results)
    ))
    return results

def fetch_cuwb_accelerometer_data(
    start,
    end,
    device_ids=None,
    environment_id=None,
    environment_name=None,
    device_types=['UWBTAG'],
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if device_ids is None:
        device_ids = honeycomb_io.devices.fetch_device_ids(
            device_types=device_types,
            environment_id=environment_id,
            environment_name=environment_name,
            start=start,
            end=end,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    query_list = [
        {'field': 'timestamp', 'operator': 'GTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(start)},
        {'field': 'timestamp', 'operator': 'LTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(end)}
    ]
    if device_ids is not None:
        query_list.append(
            {'field': 'device', 'operator': 'CONTAINED_BY', 'values': device_ids}
        )
    return_data = [
        'accelerometer_data_id',
        'timestamp',
        {'device': [
            'device_id',
            'part_number',
            'serial_number',
            'tag_id',
            'name'
        ]},
        'data'
    ]
    if device_ids is not None:
        logger.info('Fetching accelerometer data for devices {} for period {} to {}'.format(
            device_ids,
            start.isoformat(),
            end.isoformat()
        ))
    else:
        logger.info('Fetching all accelerometer data for period {} to {}'.format(
            start.isoformat(),
            end.isoformat()
        ))
    results = honeycomb_io.core.search_objects(
        object_name='AccelerometerData',
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
    logger.info('Fetched {} accelerometer observations'.format(
        len(results)
    ))
    return results

def fetch_cuwb_gyroscope_data(
    start,
    end,
    device_ids=None,
    environment_id=None,
    environment_name=None,
    device_types=['UWBTAG'],
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if device_ids is None:
        device_ids = honeycomb_io.devices.fetch_device_ids(
            device_types=device_types,
            environment_id=environment_id,
            environment_name=environment_name,
            start=start,
            end=end,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    query_list = [
        {'field': 'timestamp', 'operator': 'GTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(start)},
        {'field': 'timestamp', 'operator': 'LTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(end)}
    ]
    if device_ids is not None:
        query_list.append(
            {'field': 'device', 'operator': 'CONTAINED_BY', 'values': device_ids}
        )
    return_data = [
        'gyroscope_data_id',
        'timestamp',
        {'device': [
            'device_id',
            'part_number',
            'serial_number',
            'tag_id',
            'name'
        ]},
        'data'
    ]
    if device_ids is not None:
        logger.info('Fetching gyroscope data for devices {} for period {} to {}'.format(
            device_ids,
            start.isoformat(),
            end.isoformat()
        ))
    else:
        logger.info('Fetching all gyroscope data for period {} to {}'.format(
            start.isoformat(),
            end.isoformat()
        ))
    results = honeycomb_io.core.search_objects(
        object_name='GyroscopeData',
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
    logger.info('Fetched {} gyroscope observations'.format(
        len(results)
    ))
    return results

def fetch_cuwb_magnetometer_data(
    start,
    end,
    device_ids=None,
    environment_id=None,
    environment_name=None,
    device_types=['UWBTAG'],
    chunk_size=1000,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    if device_ids is None:
        device_ids = honeycomb_io.devices.fetch_device_ids(
            device_types=device_types,
            environment_id=environment_id,
            environment_name=environment_name,
            start=start,
            end=end,
            chunk_size=chunk_size,
            client=client,
            uri=uri,
            token_uri=token_uri,
            audience=audience,
            client_id=client_id,
            client_secret=client_secret
        )
    query_list = [
        {'field': 'timestamp', 'operator': 'GTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(start)},
        {'field': 'timestamp', 'operator': 'LTE', 'value': honeycomb_io.utils.to_honeycomb_datetime(end)}
    ]
    if device_ids is not None:
        query_list.append(
            {'field': 'device', 'operator': 'CONTAINED_BY', 'values': device_ids}
        )
    return_data = [
        'magnetometer_data_id',
        'timestamp',
        {'device': [
            'device_id',
            'part_number',
            'serial_number',
            'tag_id',
            'name'
        ]},
        'data'
    ]
    if device_ids is not None:
        logger.info('Fetching magnetometer data for devices {} for period {} to {}'.format(
            device_ids,
            start.isoformat(),
            end.isoformat()
        ))
    else:
        logger.info('Fetching all magnetometer data for period {} to {}'.format(
            start.isoformat(),
            end.isoformat()
        ))
    results = honeycomb_io.core.search_objects(
        object_name='MagnetometerData',
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
    logger.info('Fetched {} magnetometer observations'.format(
        len(results)
    ))
    return results

# Used by:
# process_cuwb_data.core (wf-process-cuwb-data)
# process_cuwb_data.geom_render (wf-process-cuwb-data)
def fetch_raw_cuwb_data(
        environment_name,
        start_time,
        end_time,
        read_chunk_size=None,
        device_type='UWBTAG',
        environment_assignment_info=False,
        entity_assignment_info=False
):
    if read_chunk_size is not None:
        logger.warn('Read chunk size option removed from fetch_raw_cuwb_data()')
    logger.info("Fetching raw CUWB tag device data for {} from {} to {}".format(environment_name, start_time, end_time))
    tag_assignments_df = honeycomb_io.environments.fetch_device_assignments(
        start=start_time,
        end=end_time,
        environment_id=None,
        environment_name=environment_name,
        device_type=device_type
    )
    tag_assignment_ids = tag_assignments_df.index.tolist()
    logger.info('Found {} tag assignmens for specified environment and time period'.format(
        len(tag_assignment_ids)
    ))
    data_ids = fetch_uwb_data_ids(
        datapoint_timestamp_min=start_time,
        datapoint_timestamp_max=end_time,
        assignment_ids=tag_assignment_ids
    )
    logger.info('Found {} UWB data points for these tag assignments and specified time period'.format(
        len(data_ids)
    ))
    df_list = list()
    for data_id in data_ids:
        data_id_df = fetch_uwb_data_data_id(
            data_id=data_id
        )
        df_list.append(data_id_df)
    df = pd.concat(df_list)
    if len(df) == 0:
        return df
    df.drop(
        columns=[
            'memory',
            'flags',
            'minutes_remaining',
            'processor_usage',
            'network_time'
        ],
        inplace=True,
        errors='ignore'
    )
    df.rename(
        columns={
            'serial_number': 'device_serial_number'
        },
        inplace=True
    )
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df.dropna(subset=['timestamp'], inplace=True)
    df.set_index('timestamp', inplace=True)
    df.sort_index(inplace=True)
    device_data = fetch_cuwb_tag_device_data(device_type=device_type)
    df = df.join(device_data.reset_index().set_index(
        'device_serial_number'), on='device_serial_number')
    df = df.reindex(columns=[
        'type',
        'device_id',
        'device_part_number',
        'device_serial_number',
        'device_name',
        'device_tag_id',
        'device_mac_address',
        'battery_percentage',
        'temperature',
        'x',
        'y',
        'z',
        'scale',
        'anchor_count',
        'quality',
        'smoothing'
    ])
    if environment_assignment_info:
        df = add_environment_assignment_info(df)
    if entity_assignment_info:
        df = add_entity_assignment_info(df)
    return df

def fetch_cuwb_tag_device_data(
        device_type='UWBTAG'
):
    logger.info('Fetching CUWB tag device data')
    client = minimal_honeycomb.MinimalHoneycombClient()
    result = client.request(
        request_type="query",
        request_name='findDevices',
        arguments={
            'device_type': {
                'type': 'DeviceType',
                'value': device_type
            }
        },
        return_object=[
            {'data': [
                'device_id',
                'part_number',
                'serial_number',
                'name',
                'tag_id',
                'mac_address'
            ]}
        ]
    )
    logger.info('Found CUWB device data for {} devices'.format(
        len(result.get('data'))))
    df = pd.DataFrame(result.get('data'))
    df.rename(
        columns={
            'part_number': 'device_part_number',
            'serial_number': 'device_serial_number',
            'name': 'device_name',
            'tag_id': 'device_tag_id',
            'mac_address': 'device_mac_address'
        },
        inplace=True
    )
    df.set_index('device_id', inplace=True)
    return df


def fetch_cuwb_tag_assignments(
        device_type='UWBTAG',
        assignment_field_name='assignments',
        assignment_id_field_name='assignment_id'
):
    logger.info('Fetching CUWB tag assignment IDs for {}'.format(
        assignment_field_name))
    client = minimal_honeycomb.MinimalHoneycombClient()
    result = client.request(
        request_type="query",
        request_name='findDevices',
        arguments={
            'device_type': {
                'type': 'DeviceType',
                'value': device_type
            }
        },
        return_object=[
            {'data': [
                'device_id',
                {assignment_field_name: [
                    assignment_id_field_name,
                    'start',
                    'end',
                ]}
            ]}
        ]
    )
    logger.info('Found {} {}'.format(
        len(result.get('data')),
        assignment_field_name
    ))
    if len(result.get('data')) == 0:
        raise ValueError('No devices of type {} found'.format(device_type))
    assignments_dict = {
        device['device_id']: device[assignment_field_name] for device in result.get('data')}
    for device_id in assignments_dict.keys():
        num_assignments = len(assignments_dict[device_id])
        # Convert timestamp strings to Pandas datetime objects
        for assignment_index in range(num_assignments):
            assignments_dict[device_id][assignment_index]['start'] = pd.to_datetime(
                assignments_dict[device_id][assignment_index]['start'],
                utc=True
            )
            assignments_dict[device_id][assignment_index]['end'] = pd.to_datetime(
                assignments_dict[device_id][assignment_index]['end'],
                utc=True
            )
        # Sort assignment list by start time
        assignments_dict[device_id] = sorted(
            assignments_dict[device_id],
            key=lambda assignment: assignment['start']
        )
        # Check integrity of assignment list
        if num_assignments > 1:
            for assignment_index in range(1, num_assignments):
                if pd.isna(assignments_dict[device_id]
                           [assignment_index - 1]['end']):
                    raise ValueError('Assignment {} starts at {} but previous assignment for this device {} starts at {} and has no end time'.format(
                        assignments_dict[device_id][assignment_index][assignment_id_field_name],
                        assignments_dict[device_id][assignment_index]['start'],
                        assignments_dict[device_id][assignment_index -
                                                    1][assignment_id_field_name],
                        assignments_dict[device_id][assignment_index - 1]['start']
                    ))
                if assignments_dict[device_id][assignment_index]['start'] < assignments_dict[device_id][assignment_index - 1]['end']:
                    raise ValueError('Assignment {} starts at {} but previous assignment for this device {} starts at {} and ends at {}'.format(
                        assignments_dict[device_id][assignment_index][assignment_id_field_name],
                        assignments_dict[device_id][assignment_index]['start'],
                        assignments_dict[device_id][assignment_index -
                                                    1][assignment_id_field_name],
                        assignments_dict[device_id][assignment_index - 1]['start'],
                        assignments_dict[device_id][assignment_index - 1]['end']
                    ))
    return assignments_dict

def add_environment_assignment_info(df):
    # Fetch environment assignment IDs (devices to environment)
    environment_assignments = fetch_cuwb_tag_assignments(
        assignment_field_name='assignments',
        assignment_id_field_name='assignment_id'
    )
    # Add environment assignment IDs to dataframe
    df = add_assignment_ids(
        df=df,
        assignments_dict=environment_assignments,
        lookup_field_name='device_id',
        assignment_field_name='assignment_id'
    )
    return df


def add_entity_assignment_info(df):
    # Fetch entity assignment IDs (trays and people to devices)
    entity_assignments = fetch_cuwb_tag_assignments(
        assignment_field_name='entity_assignments',
        assignment_id_field_name='entity_assignment_id'
    )
    # Add entity assignments IDs to dataframe
    df = add_assignment_ids(
        df=df,
        assignments_dict=entity_assignments,
        lookup_field_name='device_id',
        assignment_field_name='entity_assignment_id'
    )
    # Fetch entity info (tray and person info)
    entity_info = honeycomb_io.devices.fetch_entity_info()
    # Add entity info to dataframe
    df = df.join(entity_info, on='entity_assignment_id')
    # Fetch material assignment IDs (trays to materials)
    material_assignments = honeycomb_io.materials.fetch_material_assignments()
    # Add material assignment IDs to dataframe
    df = add_assignment_ids(
        df=df,
        assignments_dict=material_assignments,
        lookup_field_name='tray_id',
        assignment_field_name='material_assignment_id'
    )
    # Fetch material names
    material_names = honeycomb_io.materials.fetch_material_names()
    # Add material names to dataframe
    df = df.join(material_names, on='material_assignment_id')
    return df

def add_assignment_ids(
        df,
        assignments_dict,
        lookup_field_name='device_id',
        assignment_field_name='assignment_id'
):
    df = df.copy()
    df[assignment_field_name] = None
    for lookup_value, assignments in assignments_dict.items():
        if len(assignments) > 0:
            lookup_boolean = (df[lookup_field_name] == lookup_value)
            for assignment in assignments:
                if pd.isnull(assignment['start']):
                    start_boolean = True
                else:
                    start_boolean = (df.index > assignment['start'])
                if pd.isnull(assignment['end']):
                    end_boolean = True
                else:
                    end_boolean = (df.index < assignment['end'])
                df.loc[
                    lookup_boolean & start_boolean & end_boolean,
                    assignment_field_name
                ] = assignment[assignment_field_name]
    return df

# Used by:
# process_cuwb_data.core (wf-process-cuwb-data)
def fetch_material_tray_devices_assignments(environment_id, start_time, end_time):

    hc_start_time = honeycomb_io.utils.to_honeycomb_datetime(start_time)
    hc_end_time = honeycomb_io.utils.to_honeycomb_datetime(end_time)

    logger.info('Fetching CUWB tag device data')
    client = minimal_honeycomb.MinimalHoneycombClient()
    result = client.request(
        request_type="query",
        request_name='searchAssignments',
        arguments={'query': {
            'type': 'QueryExpression!',
            'value':
                {
                    'operator': 'AND',
                    'children': [
                        {'operator': 'EQ', 'field': "environment", 'value': environment_id},
                        {'operator': 'LTE', 'field': "start", 'value': hc_end_time},
                        {
                            'operator': 'OR',
                            'children': [
                                {'operator': 'GTE', 'field': "end", 'value': hc_start_time},
                                {'operator': 'ISNULL', 'field': "end", 'value': hc_end_time}
                            ]
                        }
                    ]
                }
        }},
        return_object=[
            {'data': [
                {'environment': [
                    'environment_id',
                    'name'
                ]},
                'assignment_id',
                'start',
                'end',
                {'assigned': [
                    {'... on Material': [
                        'material_id',
                        'name',
                        'description',
                        {'material_assignments': [
                            'material_assignment_id',
                            'start',
                            'end',
                            {'tray': [
                                'tray_id',
                                'name',
                                {'entity_assignments': [
                                    {'device': [
                                        'device_id',
                                        'name'
                                    ]}
                                ]}
                            ]}
                        ]}
                    ]}
                ]}
            ]}
        ]
    )
    logger.info('Found material/tray/device assignments: {} records'.format(
        len(result.get('data'))))

    df_env_assignments = pd.json_normalize(result.get('data'))
    df_env_assignments = df_env_assignments[df_env_assignments['assigned.material_assignments'].notnull()]

    records = {}
    for _, env_assignment in df_env_assignments.iterrows():
        for material_assignment in env_assignment['assigned.material_assignments']:
            tray = material_assignment['tray']

            if (honeycomb_io.utils.from_honeycomb_datetime(material_assignment['start']) > end_time or
                    (
                material_assignment['end'] is not None and
                honeycomb_io.utils.from_honeycomb_datetime(material_assignment['end']) < start_time
            )):
                continue

            for entity_assignment in tray['entity_assignments']:
                device = entity_assignment['device']
                records[env_assignment['assignment_id']] = {
                    'start': env_assignment['start'],
                    'end': env_assignment['end'],
                    'material_id': env_assignment['assigned.material_id'],
                    'material_name': env_assignment['assigned.name'],
                    'material_description': env_assignment['assigned.description'],
                    'tray_id': tray['tray_id'],
                    'tray_name': tray['name'],
                    'tray_device_id': device['device_id'],
                    'tray_device_name': device['name'],
                }

    df = pd.DataFrame.from_dict(records, orient='index')
    return df

# Used by:
# process_pose_data.process (wf-process-pose-data)
def fetch_uwb_data_data_id(
    data_id,
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
    result=client.request(
        request_type='query',
        request_name='getDatapoint',
        arguments={
            'data_id': {
                'type': 'ID!',
                'value': data_id
            }
        },
        return_object = [
            'timestamp',
            {'source': [
                {'... on Assignment': [
                    'assignment_id'
                ]}
            ]},
            {'file': [
                'data'
            ]}
        ]
    )
    datapoint_timestamp=honeycomb_io.utils.from_honeycomb_datetime(result.get('timestamp'))
    assignment_id=result.get('source', {}).get('assignment_id')
    data_jsonl_json = result.get('file', {}).get('data')
    if data_jsonl_json is None:
        logger.warn('No UWB data returned')
        return pd.DataFrame()
    try:
        data_jsonl = json.loads(data_jsonl_json)
    except:
        raise ValueError('Expected JSONL wrapped as JSON, but JSON deserialization failed')
    if not isinstance(data_jsonl, str):
        raise ValueError('Expected JSONL but got type \'{}\''.format(type(data_jsonl)))
    data_dict_list = list()
    for data_jsonl_line in data_jsonl.split('\n'):
        if len(data_jsonl_line) == 0:
            continue
        try:
            data_dict_list.append(json.loads(data_jsonl_line))
        except:
            logger.warn('Encountered malformed JSONL line. Omitting: {}'.format(
                data_jsonl_line
            ))
            continue
    df = pd.DataFrame(data_dict_list)
    original_columns = df.columns.tolist()
    df['assignment_id'] = assignment_id
    new_columns = ['assignment_id'] + original_columns
    df = df.reindex(columns=new_columns)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    if df['timestamp'].isna().any():
        logger.warn('Returned UWB data is missing some timestamp data')
    df = df.dropna(subset=['timestamp']).reset_index(drop=True)
    logger.info('Datapoint {} with timestamp {} yielded {} observations from {} to {}. Serial numbers: {}. Types: {}'.format(
        data_id,
        datapoint_timestamp.isoformat(),
        len(df),
        df['timestamp'].min().isoformat(),
        df['timestamp'].max().isoformat(),
        df['serial_number'].value_counts().to_dict(),
        df['type'].value_counts().to_dict()
    ))
    return df

# Used by:
# process_pose_data.process (wf-process-pose-data)
def extract_position_data(
    df
):
    if len(df) == 0:
        return df
    df = df.loc[df['type'] == 'position'].copy().reset_index(drop=True)
    if len(df) != 0:
        df['x_position'] = df['x'] / 1000.0
        df['y_position'] = df['y'] / 1000.0
        df['z_position'] = df['z'] / 1000.0
        df['anchor_count'] = pd.to_numeric(df['anchor_count']).astype('Int64')
        df['quality'] = pd.to_numeric(df['quality']).astype('Int64')
    df = df.reindex(columns=[
        'assignment_id',
        'timestamp',
        'object_id',
        'serial_number',
        'x_position',
        'y_position',
        'z_position',
        'anchor_count',
        'quality'
    ])
    return df

# Used by:
# process_pose_data.process (wf-process-pose-data)
def fetch_uwb_data_ids(
    datapoint_timestamp_min,
    datapoint_timestamp_max,
    assignment_ids,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    query_list = [
        {'field': 'timestamp', 'operator': 'GTE', 'value': datapoint_timestamp_min},
        {'field': 'timestamp', 'operator': 'LTE', 'value': datapoint_timestamp_max},
        {'field': 'source', 'operator': 'IN', 'values': assignment_ids}
    ]
    return_data = [
        'data_id'
    ]
    result = honeycomb_io.core.search_objects(
        object_name='Datapoint',
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
    data_ids = [datum.get('data_id') for datum in result]
    return data_ids

# Used by:
# process_pose_data.process (wf-process-pose-data)
def fetch_person_tag_info(
    start,
    end,
    environment_id,
    chunk_size=100,
    client=None,
    uri=None,
    token_uri=None,
    audience=None,
    client_id=None,
    client_secret=None
):
    query_list = [
        {'field': 'environment', 'operator': 'EQ', 'value': environment_id},
        {'field': 'start', 'operator': 'LTE', 'value': end},
        {'operator': 'OR', 'children': [
            {'field': 'end', 'operator': 'ISNULL'},
            {'field': 'end', 'operator': 'GTE', 'value': start},
        ]},
        {'field': 'assigned_type', 'operator': 'EQ', 'value': 'DEVICE'}
    ]
    return_data = [
        'assignment_id',
        'start',
        'end',
        {'assigned': [
            {'... on Device': [
                'device_id',
                'device_type',
                'name',
                'tag_id',
                {'entity_assignments': [
                    'start',
                    'end',
                    'entity_type',
                    {'entity': [
                        {'...on Person': [
                            'person_id',
                            'person_type',
                            'name',
                            'short_name'
                        ]}
                    ]}
                ]}
            ]}
        ]}
    ]
    result = honeycomb_io.core.search_objects(
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
    result = list(filter(
        lambda assignment: assignment.get('assigned', {}).get('device_type') == 'UWBTAG',
        result
    ))
    for assignment in result:
        assignment['assigned']['entity_assignments'] = minimal_honeycomb.filter_assignments(
            assignments=assignment['assigned']['entity_assignments'],
            start_time=start,
            end_time=end
        )
        if len(assignment['assigned']['entity_assignments']) > 1:
            raise ValueError('UWB tag {} has multiple entity assignments in the specified period ({} to {})'.format(
                assignment.get('assigned', {}).get('name'),
                start.isoformat(),
                end.isoformat()
            ))
    result = list(filter(
        lambda assignment: len(assignment.get('assigned', {}).get('entity_assignments')) == 1,
        result
    ))
    result = list(filter(
        lambda assignment: assignment.get('assigned', {}).get('entity_assignments')[0].get('entity_type') == 'PERSON',
        result
    ))
    data_list=list()
    for assignment in result:
        data_list.append({
            'assignment_id': assignment.get('assignment_id'),
            'device_id': assignment.get('assigned', {}).get('device_id'),
            'device_name': assignment.get('assigned', {}).get('name'),
            'tag_id': assignment.get('assigned', {}).get('tag_id'),
            'person_id': assignment.get('assigned', {}).get('entity_assignments')[0].get('entity', {}).get('person_id'),
            'person_type': assignment.get('assigned', {}).get('entity_assignments')[0].get('entity', {}).get('person_type'),
            'person_name': assignment.get('assigned', {}).get('entity_assignments')[0].get('entity', {}).get('name'),
            'short_name': assignment.get('assigned', {}).get('entity_assignments')[0].get('entity', {}).get('short_name')
        })
    df = pd.DataFrame(data_list)
    df.set_index('assignment_id', inplace=True)
    return df

def add_person_tag_info(
    uwb_data_df,
    person_tag_info_df
):
    uwb_data_df = uwb_data_df.join(person_tag_info_df, on='assignment_id')
    return uwb_data_df
