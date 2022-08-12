ENDPOINT_MAPPING = {
    'create_contact': {
        'endpoint': 'contact',
        'required_column': []
    },
    'create_list': {
        'endpoint': 'lists',
        'required_column': ['name']
    },
    'add_contact_to_list': {
        'endpoint': 'lists/{list_id}/add',
        'required_column': ['list_id', 'vids', 'emails']
    },
    'remove_contact_from_list': {
        'endpoint': 'lists/{list_id}/remove',
        'required_column': ['list_id', 'vids']
    },
    'update_contact': {
        'endpoint': 'contact/vid/{vid}/profile',
        'required_column': ['vid']
    },
    'update_contact_by_email': {
        'endpoint': 'contact/email/{email}/profile',
        'required_column': ['email']
    }
}