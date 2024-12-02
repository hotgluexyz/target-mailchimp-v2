"""MailChimp-V2 target sink class, which handles writing streams."""


import datetime
import hashlib
import json
import mailchimp_marketing as MailchimpMarketing
import requests
from mailchimp_marketing.api_client import ApiClientError, ApiClient
from singer_sdk.sinks import BatchSink
from target_hotglue.client import HotglueBatchSink, HotglueSink, HotglueBaseSink


class BaseSink(HotglueBaseSink):
    server = None
    contact_names = ["customers", "contacts", "customer", "contact"]

    @property
    def name(self) -> str:
        return self.stream_name

    @property
    def base_url(self) -> str:
        return ""

    def get_server_meta_data(self):
        header = {"Authorization": f"OAuth {self.config.get('access_token')}"}
        metadata = requests.get(
            "https://login.mailchimp.com/oauth2/metadata", headers=header
        ).json()

        if "error" in metadata:
            raise Exception(metadata["error"])

        return metadata["dc"]

    def get_server(self):
        if self.server is None:
            self.server = self.get_server_meta_data()

        return self.server

    def get_list_id(self):
        client = self.get_client()
        response = client.lists.get_all_lists()
        self.logger.info(response)

        config_name = self.config.get("list_name")
        if "lists" in response:
            for row in response["lists"]:
                # Handle case where they don't set a list_name in config
                if not config_name:
                    return row["id"]

                # NOTE: Making case insensitive to avoid issues
                if row["name"].lower() == config_name.lower():
                    return row["id"]
                
    def clean_convert(self, input):
        allowed_values = [0, "", False]
        if isinstance(input, list):
            return [self.clean_convert(i) for i in input]
        elif isinstance(input, dict):
            output = {}
            for k, v in input.items():
                v = self.clean_convert(v)
                if isinstance(v, list):
                    output[k] = [i for i in v if i or i in allowed_values]
                elif v or v in allowed_values:
                    output[k] = v
            return output
        elif input or input in allowed_values:
            if isinstance(input, (datetime.date, datetime.datetime)):
                return input.isoformat()
            return input

    def get_client(self) -> MailchimpMarketing.Client:
        if not hasattr(self, '_client') or self._client is None:
            self._client = MailchimpMarketing.Client()
            self._client.set_config(
                {
                    "access_token": self.config.get("access_token"),
                    "server": self.get_server(),
                }
            )
        return self._client
    
    def get_client_API(self) -> MailchimpMarketing.ApiClient:
        client = MailchimpMarketing.ApiClient()
        client.set_config(
            {
                "access_token": self.config.get("access_token"),
                "server": self.get_server(),
            }
        )
        return client

class MailChimpV2Sink(BaseSink, HotglueBatchSink):
    max_size = 500  # Max records to write in one batch
    list_id = None
    server = None
    custom_fields = None
    external_ids_dict = {}

    @property
    def endpoint(self) -> str:
        raise ""

    @property
    def unified_schema(self):
        return None

    groups_dict = None

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    def start_batch(self, context: dict) -> None:
        try:
            self.list_id = self.get_list_id()
        except ApiClientError as error:
            self.logger.exception("Error: {}".format(error.text))
        
    def handle_custom_fields(self, client: MailchimpMarketing.Client, record_custom_fields, merge_fields):
        #Check and populate custom fields as merge fields
        if self.custom_fields is None:
            _merge_fields = client.lists.get_list_merge_fields(self.list_id)
            self.custom_fields = {field["name"]: field["tag"] for field in _merge_fields["merge_fields"]}

        if not isinstance(record_custom_fields, list):
            self.logger.info(f"Skipping custom fields, custom fields should be a list of dicts")
        for field in record_custom_fields:
            if not isinstance(field, dict):
                self.logger.info(f"Custom field format is incorrect, skipping custom field {field}")
                self.logger.info("Custom field should follow this format: {'name': 'field name', 'value': 'field value'}.")
                continue
            
            field_name = field.get("name")
            # Note: merge fields should be sent with the tag name, not the custom field name
            # check if custom field name corresponds to a merge field tag
            if field_name in self.custom_fields.values():
                merge_fields[field_name] = field.get("value")
            # check if custom field name corresponds to a merge field name and get the tag   
            elif field_name in self.custom_fields:
                merge_fields[self.custom_fields[field_name]] = field.get("value")
            # if custom field doesn't exist create it
            else:
                merge_field_res = client.lists.add_list_merge_field(self.list_id,{"name":field_name,"type":"text"})
                self.custom_fields.update({merge_field_res["name"]: merge_field_res["tag"]})
        return merge_fields

    def process_batch_record(self, record: dict, index: int) -> dict:
        if self.stream_name.lower() in self.contact_names:
            if record.get("name"):
                first_name, *last_name = record["name"].split()
                last_name = " ".join(last_name)
            else:
                first_name = record.get("first_name") or ""
                last_name = record.get("last_name") or ""

            location = {
                "latitude": 0,
                "longitude": 0,
                "gmtoff": 0,
                "dstoff": 0,
                "country_code": "",
                "timezone": "",
                "region": "",
            }

            # validate if email has been provided, it's a required field
            email_key = "email" if "email" in record else "email_address"
            if not record.get(email_key):
                return({"error":"Email was not provided and it's a required value", "externalId": record.get("externalId")})
            
            address = None
            addresses = record.get("addresses")
            if addresses:
                # sometimes it comes as a dict
                if not isinstance(addresses, list):
                    return({"error":"Addresses field is not formatted correctly, addresses format should be: [{'line1': 'xxxxx', 'city': 'xxxxx', 'state':'xxxxx', 'postal_code':'xxxxx'}]", "externalId": record.get("externalId")})
                
                address_dict = record["addresses"][0]

                address = {
                    "addr1": address_dict.get("line1"),
                    "city": address_dict.get("city"),
                    "state": address_dict.get("state"),
                    "zip": address_dict.get("postalCode", address_dict.get("postal_code")),
                }

                if address_dict.get("country"):
                    address["country"] = address_dict.get("country")
                
                if address_dict.get("line2"):
                    address["addr2"] = address_dict.get("line2")

                # mailchimp has a strict validation on adress types addr1, city, state and zip fields must be populated
                required_fields = ["addr1", "city", "state", "zip"]
                for key, value in address.items():
                    if key in required_fields and not value:
                        self.logger.info(f"Ignoring address due to empty or missing required fields: line1, city, zip, postal_code for record with email {record['email']}")
                        address = None

                location.update(
                    {
                        "country_code": address_dict.get("country"),
                        "region": address_dict.get("state"),
                        "latitude": float(address_dict.get("latitude", 0)),
                        "longitude": float(address_dict.get("longitude", 0)),
                    }
                )        
                
            subscribed_status = self.config.get("subscribe_status", "subscribed")

            # override status if it is found in the record
            if record.get("subscribe_status"):
                subscribed_status = record.get("subscribe_status")

            # Build member dictionary and adds merge_fields without content
            member_dict = {
                "email_address": record[email_key],
                "status": subscribed_status,
                "merge_fields": {},
                "location": location,
            }
            merge_fields = {
                "FNAME": first_name,
                "LNAME": last_name,
            }
            
            if address:
                merge_fields["ADDRESS"] = address

            # add phone number if exists
            if record.get("phone_numbers"):
                phone_dict = record["phone_numbers"][0]
                if phone_dict.get("number"):
                    merge_fields.update({"PHONE": phone_dict.get("number")})

            # initialize server
            client = self.get_client()
            
            merge_fields = self.handle_custom_fields(client, record.get("custom_fields", []), merge_fields)

            # Iterate through all of the possible merge fields, if one is None
            # then it is removed from the dictionary
            keys_to_remove = []
            for field, value in merge_fields.items():
                if value == None:
                    keys_to_remove.append(field)
            
            for key in keys_to_remove:
                merge_fields.pop(key)

            member_dict["merge_fields"] = merge_fields

            # add email and externalid to externalid dict for state
            self.external_ids_dict[record[email_key]] = record.get("externalId", record[email_key])

            # add groups 
            lists = record.get("lists")
            if record.get("lists"):
                group_names = {}
                # get groups names and ids
                if self.groups_dict is None and self.list_id:
                    # get the group titles - interest categories
                    group_titles = client.api_client.call_api(f"/lists/{self.list_id}/interest-categories", "GET")
                    group_titles = group_titles["categories"]
                    group_names.update({g_title["title"]: {"id": g_title["id"], "group_names": {}} for g_title in group_titles})
                    # get all group names(interests) ids for each group title
                    for group_title in group_titles:
                        interests = client.api_client.call_api(f"/lists/{self.list_id}/interest-categories/{group_title['id']}/interests", "GET")
                        group_names[group_title["title"]]["group_names"] = {group_name["name"]: group_name["id"] for group_name in interests["interests"]}
                
                    self.groups_dict = group_names

                # get each groupName in lists id
                group_name_ids = []
                for list_name in lists:
                    try:
                        group_title, group_name = list_name.split("/")
                    except:
                        return({"error":f"Failed to post: List item '{list_name}' format is incorrect or incomplete, list item format should be 'groupTitle/groupName'", "externalId": record.get("externalId")})
                    
                    # check if group title exists
                    if self.groups_dict.get(group_title):
                        # get group name id and add it to the payload
                        if not self.groups_dict[group_title]["group_names"].get(group_name):
                            body = {"name": group_name}
                            self.logger.info(f"Creating GroupName {group_name} inside category {group_title}")
                            new_group_name = client.api_client.call_api(f"/lists/{self.list_id}/interest-categories/{self.groups_dict[group_title]['id']}/interests", "POST", body=body)
                            self.groups_dict[group_title]["group_names"].update({new_group_name["name"]: new_group_name["id"]})
                        # add group name to lists_ids for payload
                        group_name_ids.append(self.groups_dict[group_title]["group_names"][group_name])
                    else:
                        return({"error":f"Group title {group_title} not found in this account.", "externalId": record.get("externalId")})                

                member_dict["interests"] = {group_name_id: True for group_name_id in group_name_ids}

            # clean null values
            member_dict = self.clean_convert(member_dict)
            if record.get("tags"):
                member_dict['tags'] = record.get('tags', [])
            return member_dict

    def make_batch_request(self, records):
        if self.stream_name.lower() in self.contact_names:
            if self.list_id is not None:
                if records:
                    try:
                        client = self.get_client()

                        response = client.lists.batch_list_members(
                            self.list_id,
                            {"members": records, "update_existing": True},
                        )

                        return response
                    except ApiClientError as error:
                        raise Exception("Error: None of the records went through due to error '{}', no state available.".format(error.text))
                else:
                    return {}
            else:
                raise Exception(
                    f"Failed to post because there was no list ID found for the list name {self.config.get('list_name')}!"
                )

    def handle_batch_response(self, response, map_errors) -> dict:
        """
        This method should return a dict.
        It's recommended that you return a key named "state_updates".
        This key should be an array of all state updates
        """
        state_updates = []
        members = response.get("new_members", []) + response.get("updated_members", [])

        for member in members:
            state_updates.append({
                "success": True,
                "id": member["id"],
                "externalId": self.external_ids_dict.get(member.get("email_address"))
            })

        for error in response.get("errors", []):
            state_updates.append({
                "success": False,
                "error": error.get("error"),
                "externalId": self.external_ids_dict.get(error.get("email_address"))
            })
        
        for map_error in map_errors:
            state_updates.append({
                "success": False,
                "error": map_error.get("error"),
                "externalId": map_error.get("externalId")
            })

        return {"state_updates": state_updates}


    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        raw_records = context.get("records", [])
        if not raw_records:
            return

        records = list(map(lambda e: self.process_batch_record(e[1], e[0]), enumerate(raw_records)))

        map_errors = [rec for rec in records if "error" in rec]
        records = [rec for rec in records if "error" not in rec]

        response = self.make_batch_request(records)

        result = self.handle_batch_response(response, map_errors)

        for state in result.get("state_updates", list()):
            self.update_state(state)


class FallbackSink(BaseSink, HotglueSink):
    """Precoro target sink class."""

    @property
    def base_url(self) -> str:
        return ""

    @property
    def endpoint(self) -> str:
        return f"/{self.stream_name}"

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        if self.stream_name.lower() in self.contact_names:
            if not record.get("status"):
                self.logger.info(
                    f"Status not found for record {record}, adding status_if_new as subscribed by default"
                )
                record["status_if_new"] = "subscribed"
        return record

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        if not record:
            return "", False, state_updates
        
        method = "POST"
        endpoint = self.endpoint
        client_api = self.get_client_API()
        client = self.get_client()
        id = record.pop("id", None)
        
        # custom logic for contacts
        if self.stream_name.lower() in self.contact_names:
            # add email to the endpoint to use create or update endpoint
            email = record.get("email_address")
            if not email:
                raise Exception(
                    f"No email found for record {record}, email is a required field."
                )
            # get list id
            list_id = self.get_list_id()
            if not list_id:
                raise Exception(
                    f"No list id found to send record with email {email} for list {self.config.get('list_name')}"
                )
            # using create or update endpoint for contacts
            method = "PUT"
            endpoint = f"/lists/{list_id}/members/{email}"
            self.get_merge_fields(record, list_id)
        elif id:
            endpoint = f"{endpoint}/{id}"

        json_serializable_record = self.clean_convert(record)
        try:
            response = client_api.call_api(
                resource_path=endpoint, method=method, body=json_serializable_record
            )
            id = response["id"] if "id" in response else id if id else ""
            return id, True, state_updates
        except Exception as e:
            self.logger.exception(f"Error when upserting record {record} on endpoint {endpoint}: {e}")
            raise e

    def get_merge_fields(self, record: dict, list_id: str):
        merge_fields = self.get_client().lists.get_list_merge_fields(list_id)
        record["merge_fields"] = {}
        for merge_field in merge_fields["merge_fields"]:
            merge_field_tag = "merge_fields." + merge_field["tag"]
            if "merge_fields." + merge_field["tag"] in record:
                record["merge_fields"][merge_field["tag"]] = record["merge_fields." + merge_field["tag"]]
        merge_fields_to_remove = [field for field in record if field.startswith("merge_fields.")]
        for field in merge_fields_to_remove:
            record.pop(field)
        