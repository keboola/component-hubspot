# Keboola Hubspot Writer  
  
The HubSpot CRM helps companies grow traffic, convert leads, get insights to close more deals, etc.  
Keboola Hubspot Writer will provide users the assistance to maintain and manage their contacts within Hubspot.  
  
## Requirement  
There are two ways to authorize for Hubspot API.

 1. **Hapikey** - Your Hubspot API key, This will be soon deprecated, see more ([here](https://developers.hubspot.com/changelog/upcoming-api-key-sunset))  
 2. **Private App Token** - See how to create and assign permissions ([here](https://developers.hubspot.com/docs/api/private-apps))
  
## Configuration  
  
First fill in Authorization configuration. Then click ADD ROW button and fill in the name by which you can easily identify the configuration row. Next fill in the Desired object you want to manipulate and then select the action you want to perform with this object. 
The writer will fetch all input tables and convert each row into the required request format based on the requirements of the endpoint. Each writer will write to `one` endpoint.   
  
#### Column names
* The following file contains list of available columns/properties of Hubspot objects (not contains manually created properties) [Available objects properties](https://bitbucket.org/kds_consulting_team/kds-team.wr-hubspot/src/master/docs/objects_properties.md)

| Batch endpoint | Object          | Object Action    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | Required Columns                                                          |  
|----------------|-----------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------|  
| Yes            | Contact         | Create           | A list of contact properties that you want to set for the new contact record. Each entry in the list must include the internal name of the property, and the value that you want to set for that property. Note: You must include at least one property for the new contact, or you will receive an error.                                                                                                                                                                                                                                                                                    |                                                                           |
| No             | Contact         | Add to List      | It is required to have both `vids` and `emails` columns in the input data file. The code will prioritize the values input for `vids`. Example, if there are inputs for both `emails` and `vids` in the same row, the code will prioritize `vids` pushing this value into the request and ignore the value in `emails`. Please note that you cannot manually add contacts to dynamic lists. To determine whether a list is dynamic or static, when you get a list, you will see a flag called dynamic that equates to true or false. Up to 500 total contacts can be added in a single request. | `list_id, vids, emails`                                                   |
| Yes            | Contact         | Update           | This endpoint is used to update existing contacts, and it requires `vid` in the input data file. `vid` cannot be empty. The rest of the columns other than vid will be used as a request parameter to update the contact property. If either the property or the contact does not exist, the update request for that specific contact will fail.                                                                                                                                                                                                                                              | `vid`                                                                     | 
| No             | Contact         | Update by Email  | Like the `Update Contact` endpoint, this endpoint is used to update existing contacts, and it requires `email` in the "inputdata" file. The rest of the columns will be used to update the contact's properties                                                                                                                                                                                                                                                                                                                                                                               | `email`                                                                   |  
| Yes            | Contact         | Remove from List | Please note that you cannot manually remove contacts from dynamic lists - they can only be updated by the contacts system based on the properties of the list itself. To determine whether a list is dynamic or static, when you get a list, you will see a flag called dynamic that equates to true or false.                                                                                                                                                                                                                                                                                | `list_id, vids`                                                           |   
| Yes            | Company         | Create           | Creates companies with defined properties if any are present.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `name`                                                                    |  
| Yes            | Company         | Update           | Updates companies identified by `company_id` with defined properties.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | `company_id`                                                              |
| Yes            | Company         | Remove           | Deletes companies identified by `company_id`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `company_id`                                                              |
| No             | Company         | Add to List      | Adds companies to lists                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | `list_id, vids`                                                           |
| No             | Company         | Remove from List | Removes companies from lists                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `list_id, vids`                                                           |
| No             | Contact List    | Create           | Creates contact lists. All list created via this component will `NOT` be dynamic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `name`                                                                    |
| No             | Custom List     | Create           | Creates company/deal lists. Single table can contain multiple types indicated by column object_type. At the moment we support `company` and `deal`. All list created will have processing type `MANUAL`.                                                                                                                                                                                                                                                                                                                                                                                      | `name, object_type`                                                       |
| Yes            | Deal            | Create           | Creates deals with defined properties if any are present.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `hubspot_owner_id`                                                        |  
| Yes            | Deal            | Update           | Updates deals identified by `deal_id` with defined properties.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | `deal_id`                                                                 |
| Yes            | Deal            | Remove           | Deletes deals identified by `deal_id`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `deal_id`                                                                 |
| No             | Deal            | Add to List      | Adds deals to lists                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `list_id, vids`                                                           |
| No             | Deal            | Remove from List | Removes deals from lists                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | `list_id, vids`                                                           |
| No             | Association     | Create           | Create association between objects (see more on object types in [HubSpot documentation](https://developers.hubspot.com/docs/api/crm/understanding-the-crm#object-type-id))                                                                                                                                                                                                                                                                                                                                                                                                                    | `from_id,to_id,from_object_type,to_object_type`                           |
| No             | Association     | Remove           | Remove association between objects (see more on object types in [HubSpot documentation](https://developers.hubspot.com/docs/api/crm/understanding-the-crm#object-type-id))                                                                                                                                                                                                                                                                                                                                                                                                                    | `from_id,to_id,from_object_type,to_object_type`                           |
| Yes            | Line item       | Create           | Create a new Line item, it have to associated to a deals, quotes, invoices, payment links or subscription (more information in [HubSpot documentation](https://developers.hubspot.com/docs/api/crm/line-items#:~:text=To%20create%20a%20line%20item,hs_product_id%20in%20the%20post%20body.))                                                                                                                                                                                                                                                                                                 | `name, price, quantity, association_id, association_category,association_type_id` |
| Yes            | Line item       | Remove           | Remove Line item identified by `line_item_id`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `line_item_id`                                                            |
| No             | Secondary Email | Add              | Adds secondary email to contact (This endpoint is experimental)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `vid, secondary_email`                                                    |
| No             | Secondary Email | Update           | Updates secondary email of contact (This endpoint is experimental)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `vid, secondary_email_old, secondary_email`                               |
| No             | Secondary Email | Remove           | Deletes secondary email of contact (This endpoint is experimental)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `vid, secondary_email`                                                    |
| No             | Custom object   | Create           | Create a custom object record, for defining the object type, is possible to use the object_type or the writer can use the input table name.   (see more on object types in [HubSpot documentation](https://developers.hubspot.com/beta-docs/guides/api/crm/objects/custom-objects))                                                                                                                                                                                                                                                                                                                                                                                                              | `object_type`                                                             |


