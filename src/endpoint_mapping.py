ENDPOINT_MAPPING = {
    'contact_create': {
        'endpoint': 'contacts/v1/contact',
        'required_column': [],
        'method': 'post'
    },
    'list_create': {
        'endpoint': 'contacts/v1/lists',
        'required_column': ['name'],
        'method': 'post'
    },
    'contact_add_to_list': {
        'endpoint': 'contacts/v1/lists/{list_id}/add',
        'required_column': ['list_id', 'vids', 'emails'],
        'method': 'post'
    },
    'contact_remove_from_list': {
        'endpoint': 'contacts/v1/lists/{list_id}/remove',
        'required_column': ['list_id', 'vids'],
        'method': 'post'
    },
    'contact_update': {
        'endpoint': 'contacts/v1/contact/vid/{vid}/profile',
        'required_column': ['vid'],
        'method': 'post'
    },
    'contact_update_by_email': {
        'endpoint': 'contacts/v1/contact/email/{email}/profile',
        'required_column': ['email'],
        'method': 'post'
    },
    'company_create': {
        'endpoint': 'companies/v2/companies',
        'required_column': ['name'],
        'method': 'post'
    },
    'company_update': {
        'endpoint': 'companies/v2/companies/{company_id}',
        'required_column': ['company_id'],
        'method': 'put'
    },
    'company_remove': {
        'endpoint': 'companies/v2/companies/{company_id}',
        'required_column': ['company_id'],
        'method': 'delete'
    }
}

# for backward compatibility
LEGACY_ENDPOINT_MAPPING_CONVERSION = {'create_contact': 'contact_create',
                                      'create_list': 'list_create',
                                      'add_contact_to_list': 'contact_add_to_list',
                                      'remove_contact_from_list': 'contact_remove_from_list',
                                      'update_contact': 'contact_update',
                                      'update_contact_by_email': 'contact_update_by_email'}
