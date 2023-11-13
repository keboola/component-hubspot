ENDPOINT_MAPPING = {
    'contact_create': {
        'endpoint': 'crm/v3/objects/contacts/batch/create',
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
        'endpoint': 'crm/v3/objects/contacts/batch/update',
        'required_column': ['vid'],
        'method': 'post'
    },
    'contact_update_by_email': {
        'endpoint': 'contacts/v1/contact/email/{email}/profile',
        'required_column': ['email'],
        'method': 'post'
    },
    'company_create': {
        'endpoint': 'crm/v3/objects/companies/batch/create',
        'required_column': ['name'],
        'method': 'post'
    },
    'company_update': {
        'endpoint': 'crm/v3/objects/companies/batch/update',
        'required_column': ['company_id'],
        'method': 'post'
    },
    'company_remove': {
        'endpoint': 'companies/v2/companies/{company_id}',
        'required_column': ['company_id'],
        'method': 'delete'
    },
    'deal_create': {
        'endpoint': 'crm/v3/objects/deals/batch/create',
        'required_column': ['hubspot_owner_id'],
        'method': 'post'
    },
    'deal_update': {
        'endpoint': 'crm/v3/objects/deals/batch/update',
        'required_column': ['deal_id'],
        'method': 'post'
    },
    'deal_remove': {
        'endpoint': 'crm/v3/objects/deals/{deal_id}',
        'required_column': ['deal_id'],
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
