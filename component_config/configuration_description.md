The component will convert the input files into appropriated request form to the respective endpoints. Different endpoints will have different required column names.

Endpoints

| Endpoint | Required Columns | Descriptions |
|-|-|-|
| create_contact | |  A list of contact properties that you want to set for the new contact record. Each entry in the list must include the internal name of the property, and the value that you want to set for that property. Note: You must include at least one property for the new contact or you will receive an error. |
| create_list | `name` | All list created via this component will NOT be dynamic. |
| add_contact_to_list | `list_id, vids, emails` | It is required to have both `vids` and `emails` columns in the input data file. The code will prioritize the values input for `vids`. Example, if there are inputs for both `emails` and `vids` in the same row, the code will prioritize `vids` pushing this value into the request and ignore the value in `emails`. Please note that you cannot manually add contacts to dynamic lists. To determine whether a list is dynamic or static, when you get a list, you will see a flag called dynamic that equates to true or false |
| remove_contact_from_list | `list_id, vids` | Please note that you cannot manually remove contacts to dynamic lists - they can only be updated by the contacts system based on the properties of the list itself. To determine whether a list is dynamic or static, when you get a list, you will see a flag called dynamic that equates to true or false. |