import inflection

SCHEMA = {
    'Datapoint': {
        'id_field_name': 'data_id'
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
