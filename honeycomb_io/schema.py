import inflection

def create_endpoint_name(object_name):
    name = 'create' + object_name
    return name

def create_endpoint_argument_name(object_name):
    name = inflection.camelize(object_name, uppercase_first_letter=False)
    return name

def create_endpoint_argument_type(object_name):
    name = object_name + 'Input'
    return name

def id_field_name(object_name):
    name =  inflection.underscore(object_name) + '_id'
    return name

def delete_endpoint_name(object_name):
    name = 'delete' + object_name
    return name
