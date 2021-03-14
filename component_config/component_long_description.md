The HubSpot CRM helps companies grow traffic, convert leads, get insights to close more deals, etc.
Keboola Hubspot Writer will provide users the assistance to maintain and manage their contacts within Hubspot.

### Requirement
Your Hubspot API token, for more information see here ([how-do-i-get-my-hubspot-api-key](https://knowledge.hubspot.com/integrations/how-do-i-get-my-hubspot-api-key))

### Configuration

The writer will fetch all input tables and convert each row into the required request format based on the requirements of the endpoint. Each writer will write to `one` endpoint. 

| Endpoint | Required Columns | Descriptions |
|-|-|-|
| Create Contact | |  A list of contact properties that you want to set for the new contact record. Each entry in the list must include the internal name of the property, and the value that you want to set for that property. Note: You must include at least one property for the new contact or you will receive an error. |
| Create List | `name` | All list created via this component will `NOT` be dynamic. |
| Add Contact to List | `list_id, vids, emails` | It is required to have both `vids` and `emails` columns in the input data file. The code will prioritize the values input for `vids`. Example, if there are inputs for both `emails` and `vids` in the same row, the code will prioritize `vids` pushing this value into the request and ignore the value in `emails`. Please note that you cannot manually add contacts to dynamic lists. To determine whether a list is dynamic or static, when you get a list, you will see a flag called dynamic that equates to true or false |
| Remove Contact from List | `list_id, vids` | Please note that you cannot manually remove contacts from dynamic lists - they can only be updated by the contacts system based on the properties of the list itself. To determine whether a list is dynamic or static, when you get a list, you will see a flag called dynamic that equates to true or false. |

