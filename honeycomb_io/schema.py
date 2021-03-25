import inflection

SCHEMA = {
    'Assignment': {
        'create_endpoint_name': 'assignToEnvironment'
    },
    'EntityAssignment': {
        'create_endpoint_name': 'assignToEntity'
    },
    'MaterialAssignment': {
        'create_endpoint_name': 'assignToMaterial'
    },
    'Datapoint': {
        'id_field_name': 'data_id'
    },
    'CoordinateSpace': {
        'id_field_name': 'space_id'
    },
    'Pose2D': {
        'search_endpoint_name': 'searchPoses2D',
        'id_field_name': 'pose_id'
    },
    'Pose3D': {
        'search_endpoint_name': 'searchPoses3D',
        'id_field_name': 'pose_id'
    },
    'PoseTrack2D': {
        'search_endpoint_name': 'searchPoseTracks2D',
        'id_field_name': 'pose_track_id'
    },
    'Pose3D': {
        'search_endpoint_name': 'searchPoseTracks3D',
        'id_field_name': 'pose_track_id'
    },
    'AccelerometerData': {
        'search_endpoint_name': 'searchAccelerometerData'
    },
    'GyroscopeData': {
        'search_endpoint_name': 'searchGyroscopeData'
    },
    'MagnetometerData': {
        'search_endpoint_name': 'searchMagnetometerData'
    }
}

def create_endpoint_name(object_name):
    name = SCHEMA.get(object_name, {}).get('create_endpoint_name')
    if name is None:
        name = 'create' + object_name
    return name

def create_endpoint_argument_name(object_name):
    name = SCHEMA.get(object_name, {}).get('create_endpoint_argument_name')
    if name is None:
        name = inflection.camelize(object_name, uppercase_first_letter=False)
    return name

def create_endpoint_argument_type(object_name):
    name = SCHEMA.get(object_name, {}).get('create_endpoint_argument_type')
    if name is None:
        name = object_name + 'Input'
    return name

def update_endpoint_name(object_name):
    name = SCHEMA.get(object_name, {}).get('update_endpoint_name')
    if name is None:
        name = 'update' + object_name
    return name

def update_endpoint_argument_name(object_name):
    name = SCHEMA.get(object_name, {}).get('update_endpoint_argument_name')
    if name is None:
        name = inflection.camelize(object_name, uppercase_first_letter=False)
    return name

def update_endpoint_argument_type(object_name):
    name = SCHEMA.get(object_name, {}).get('update_endpoint_argument_type')
    if name is None:
        name = object_name + 'UpdateInput'
    return name

def search_endpoint_name(object_name):
    name = SCHEMA.get(object_name, {}).get('search_endpoint_name')
    if name is None:
        name = 'search' + object_name + 's'
    return name

def delete_endpoint_name(object_name):
    name = SCHEMA.get(object_name, {}).get('delete_endpoint_name')
    if name is None:
        name = 'delete' + object_name
    return name

def id_field_name(object_name):
    name = SCHEMA.get(object_name, {}).get('id_field_name')
    if name is None:
        name =  inflection.underscore(object_name) + '_id'
    return name
