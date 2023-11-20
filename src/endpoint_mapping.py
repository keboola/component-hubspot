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
        'endpoint': 'crm/v3/objects/companies/batch/archive',
        'required_column': ['company_id'],
        'method': 'post'
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
        'endpoint': 'crm/v3/objects/deals/batch/archive',
        'required_column': ['deal_id'],
        'method': 'post'
    },
    'ticket_create': {
        'endpoint': 'crm/v3/objects/tickets/batch/create',
        'required_column': ['association_id', 'association_category', 'association_type_id'],
        'method': 'post'
    },
    'ticket_update': {
        'endpoint': 'crm/v3/objects/tickets/batch/update',
        'required_column': ['ticket_id'],
        'method': 'post'
    },
    'ticket_remove': {
        'endpoint': 'crm/v3/objects/tickets/batch/archive',
        'required_column': ['ticket_id'],
        'method': 'post'
    },
    'product_create': {
        'endpoint': 'crm/v3/objects/products/batch/create',
        'required_column': ['association_id', 'association_category', 'association_type_id'],
        'method': 'post'
    },
    'product_update': {
        'endpoint': 'crm/v3/objects/products/batch/update',
        'required_column': ['product_id'],
        'method': 'post'
    },
    'product_remove': {
        'endpoint': 'crm/v3/objects/products/batch/archive',
        'required_column': ['product_id'],
        'method': 'post'
    },
    'quote_create': {
        'endpoint': 'crm/v3/objects/quotes/batch/create',
        'required_column': ['association_id', 'association_category', 'association_type_id'],
        'method': 'post'
    },
    'quote_update': {
        'endpoint': 'crm/v3/objects/quotes/batch/update',
        'required_column': ['quote_id'],
        'method': 'post'
    },
    'quote_remove': {
        'endpoint': 'crm/v3/objects/quotes/batch/archive',
        'required_column': ['quote_id'],
        'method': 'post'
    },
    'line_item_create': {
        'endpoint': 'crm/v3/objects/line_items/batch/create',
        'required_column': ['association_id', 'association_category', 'association_type_id'],
        'method': 'post'
    },
    'line_item_update': {
        'endpoint': 'crm/v3/objects/line_items/batch/update',
        'required_column': ['line_item_id'],
        'method': 'post'
    },
    'line_item_remove': {
        'endpoint': 'crm/v3/objects/line_items/batch/archive',
        'required_column': ['line_item_id'],
        'method': 'post'
    },
    'tax_create': {
        'endpoint': 'crm/v3/objects/taxes/batch/create',
        'required_column': ['association_id', 'association_category', 'association_type_id'],
        'method': 'post'
    },
    'tax_update': {
        'endpoint': 'crm/v3/objects/taxes/batch/update',
        'required_column': ['tax_id'],
        'method': 'post'
    },
    'tax_remove': {
        'endpoint': 'crm/v3/objects/taxes/batch/archive',
        'required_column': ['tax_id'],
        'method': 'post'
    },
    'call_create': {
        'endpoint': 'crm/v3/objects/calls/batch/create',
        'required_column': ['association_id', 'association_category', 'association_type_id'],
        'method': 'post'
    },
    'call_update': {
        'endpoint': 'crm/v3/objects/calls/batch/update',
        'required_column': ['call_id'],
        'method': 'post'
    },
    'call_remove': {
        'endpoint': 'crm/v3/objects/calls/batch/archive',
        'required_column': ['call_id'],
        'method': 'post'
    },
    'communication_create': {
        'endpoint': 'crm/v3/objects/communications/batch/create',
        'required_column': ['association_id', 'association_category', 'association_type_id'],
        'method': 'post'
    },
    'communication_update': {
        'endpoint': 'crm/v3/objects/communications/batch/update',
        'required_column': ['communication_id'],
        'method': 'post'
    },
    'communication_remove': {
        'endpoint': 'crm/v3/objects/communications/batch/archive',
        'required_column': ['communication_id'],
        'method': 'post'
    },
    'email_create': {
        'endpoint': 'crm/v3/objects/emails/batch/create',
        'required_column': ['association_id', 'association_category', 'association_type_id'],
        'method': 'post'
    },
    'email_update': {
        'endpoint': 'crm/v3/objects/emails/batch/update',
        'required_column': ['email_id'],
        'method': 'post'
    },
    'email_remove': {
        'endpoint': 'crm/v3/objects/emails/batch/archive',
        'required_column': ['email_id'],
        'method': 'post'
    },
    'meeting_create': {
        'endpoint': 'crm/v3/objects/meetings/batch/create',
        'required_column': ['association_id', 'association_category', 'association_type_id'],
        'method': 'post'
    },
    'meeting_update': {
        'endpoint': 'crm/v3/objects/meetings/batch/update',
        'required_column': ['meeting_id'],
        'method': 'post'
    },
    'meeting_remove': {
        'endpoint': 'crm/v3/objects/meetings/batch/archive',
        'required_column': ['meeting_id'],
        'method': 'post'
    },
    'note_create': {
        'endpoint': 'crm/v3/objects/notes/batch/create',
        'required_column': ['association_id', 'association_category', 'association_type_id'],
        'method': 'post'
    },
    'note_update': {
        'endpoint': 'crm/v3/objects/notes/batch/update',
        'required_column': ['note_id'],
        'method': 'post'
    },
    'note_remove': {
        'endpoint': 'crm/v3/objects/notes/batch/archive',
        'required_column': ['note_id'],
        'method': 'post'
    },
    'postal_mail_create': {
        'endpoint': 'crm/v3/objects/postal_mail/batch/create',
        'required_column': ['association_id', 'association_category', 'association_type_id'],
        'method': 'post'
    },
    'postal_mail_update': {
        'endpoint': 'crm/v3/objects/postal_mail/batch/update',
        'required_column': ['postal_mail_id'],
        'method': 'post'
    },
    'postal_mail_remove': {
        'endpoint': 'crm/v3/objects/postal_mail/batch/archive',
        'required_column': ['postal_mail_id'],
        'method': 'post'
    },
    'task_create': {
        'endpoint': 'crm/v3/objects/tasks/batch/create',
        'required_column': ['association_id', 'association_category', 'association_type_id'],
        'method': 'post'
    },
    'task_update': {
        'endpoint': 'crm/v3/objects/tasks/batch/update',
        'required_column': ['task_id'],
        'method': 'post'
    },
    'task_remove': {
        'endpoint': 'crm/v3/objects/tasks/batch/archive',
        'required_column': ['task_id'],
        'method': 'post'
    }
}

# for backward compatibility
LEGACY_ENDPOINT_MAPPING_CONVERSION = {'create_contact': 'contact_create',
                                      'create_list': 'list_create',
                                      'add_contact_to_list': 'contact_add_to_list',
                                      'remove_contact_from_list': 'contact_remove_from_list',
                                      'update_contact': 'contact_update',
                                      'update_contact_by_email': 'contact_update_by_email'}
