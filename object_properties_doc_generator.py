import requests

OBJECTS = ["contact", "company", "list", "deal", "ticket", "product", "quote", "line_item", "tax", "call",
           "communication", "email", "meeting", "note", "postal_mail", "task", "custom_list", "association",
           "secondary_email"]
TOKEN = 'TODO'


# get all properties for each object
def get_properties(hubspot_object):
    url = f'https://api.hubapi.com/crm/v3/objects/{hubspot_object}/properties'
    querystring = {"limit": "100"}
    headers = {
        'Authorization': f'Bearer {TOKEN}',
        'Content-Type': 'application/json'
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    return response.json()


# create markdown file with hubspot object name and his properties in one file
def create_file(md):
    filename = 'docs/objects_properties.md'
    with open(filename, 'w+') as file:
        file.writelines(md)


# create markdown lines for each property
def md_lines(hubspot_object, properties):
    lines = [f'## {hubspot_object} \n\n']
    for prop in properties:
        lines.append(f'#### {prop} \n')
    return lines


object_lines = ['# Available properties/columns of Hubspot standard objects \n\n']
for h_object in OBJECTS:
    print(f'Getting properties for {h_object}')
    prop = get_properties(h_object)
    if isinstance(prop, dict):
        if prop.get('status') == 'error':
            print(f'Error: {prop.get("message")}')
            continue
    print(f'Properties for {h_object} received: {prop}')
    object_lines.extend(md_lines(h_object, prop))

create_file(object_lines)
