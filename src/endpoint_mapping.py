ENDPOINT_MAPPING = {
    'create_contact': {
        'endpoint': 'contacts/v1/contact',
        'required_column': [],
        'method': 'post'
    },
    'create_list': {
        'endpoint': 'contacts/v1/lists',
        'required_column': ['name'],
        'method': 'post'
    },
    'add_contact_to_list': {
        'endpoint': 'contacts/v1/lists/{list_id}/add',
        'required_column': ['list_id', 'vids', 'emails'],
        'method': 'post'
    },
    'remove_contact_from_list': {
        'endpoint': 'contacts/v1/lists/{list_id}/remove',
        'required_column': ['list_id', 'vids'],
        'method': 'post'
    },
    'update_contact': {
        'endpoint': 'contacts/v1/contact/vid/{vid}/profile',
        'required_column': ['vid'],
        'method': 'post'
    },
    'update_contact_by_email': {
        'endpoint': 'contacts/v1/contact/email/{email}/profile',
        'required_column': ['email'],
        'method': 'post'
    },
    'create_company': {
        'endpoint': 'companies/v2/companies',
        'required_column': ['name'],
        'method': 'post'
    },
    'update_company': {
        'endpoint': 'companies/v2/companies/{company_id}',
        'required_column': ['company_id'],
        'method': 'put'
    },
    'remove_company': {
        'endpoint': 'companies/v2/companies/{company_id}',
        'required_column': ['company_id'],
        'method': 'delete'
    }
}
