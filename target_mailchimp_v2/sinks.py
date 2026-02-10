"""MailChimp-V2 target sink class, which handles writing streams."""


import mailchimp_marketing as MailchimpMarketing
import requests
from mailchimp_marketing.api_client import ApiClientError, ApiClient
from hotglue_singer_sdk.target_sdk.client import HotglueBatchSink, HotglueSink, HotglueBaseSink
import json
from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError

def classify_batch_error_or_false(error: dict):
    if error.get("error_code") in ["ERROR_GENERIC", "HG_EMAIL_REQUIRED", "HG_ADDRESS_FORMAT_ERROR", "HG_LIST_ITEM_FORMAT_ERROR", "HG_GROUP_TITLE_NOT_FOUND", "HG_ADDRESS_MISSING_FIELDS"]:
        return {"hg_error_class": InvalidPayloadError.__name__}
    return False

def handle_call_api_error(logger, error: ApiClientError, custom_message_start: str = "", custom_message_end: str = "") -> None:

    status_code = error.status_code if hasattr(error,'status_code') else error.status if hasattr(error,'status') else None
    error_message = error.text or str(error)
    custom_error_message = custom_message_start + error_message + custom_message_end

    if status_code is not None:
        if (status_code == 401 or status_code == 403):
            raise InvalidCredentialsError(custom_error_message)
        if (status_code == 400):
            raise InvalidPayloadError(custom_error_message)

    logger.exception("Error: {}".format(custom_error_message))

    if (custom_error_message != error_message):
        raise Exception(custom_error_message)

    raise error

def get_email_if_exists(record: dict):
    # Note that Mailchimp strips whitespace from email anyways, we do it here so we map correctly to externalIds
    if record.get("email_address"):
        return record.get("email_address").strip() if isinstance(record.get("email_address"), str) else record.get("email_address")
    elif record.get("email"):
        return record.get("email").strip() if isinstance(record.get("email"), str) else record.get("email")
    else:
        return None

class BaseSink(HotglueBaseSink):
    server = None
    contact_names = ["customers", "contacts", "customer", "contact", "list_members"]
    list_id = None

    @property
    def name(self) -> str:
        return self.stream_name

    @property
    def base_url(self) -> str:
        return ""

    def get_server_meta_data(self):
        access_token = self.config.get('access_token')
        
        # Check if this is an API key (format: key-datacenter)
        # API keys contain a hyphen followed by the datacenter (e.g., "abc123-us22")
        if access_token and '-' in access_token:
            # Extract datacenter from API key
            parts = access_token.rsplit('-', 1)
            if len(parts) == 2 and parts[1]:
                return parts[1]
        
        # Otherwise, try OAuth metadata endpoint
        header = {"Authorization": f"OAuth {access_token}"}
        metadata = requests.get(
            "https://login.mailchimp.com/oauth2/metadata", headers=header
        ).json()

        if "error" in metadata:
            if (metadata["error"] == "invalid_token"):
                raise InvalidCredentialsError(metadata["error"])
            if "error_description" in metadata:
                raise Exception(metadata["error"] + ", " + metadata["error_description"])
            raise Exception(metadata["error"])

        return metadata["dc"]

    def get_server(self):
        if self.server is None:
            self.server = self.get_server_meta_data()

        return self.server

    def get_list_id(self):
        if self.list_id is None:
            try:
                client = MailchimpMarketing.Client()
                server = self.get_server()
                client.set_config(
                    {"access_token": self.config.get("access_token"), "server": server}
                )
                response = client.lists.get_all_lists()
            except ApiClientError as error:
                handle_call_api_error(self.logger, error)
            self.logger.info(response)

            config_name = self.config.get("list_name")
            if "lists" in response:
                for row in response["lists"]:
                    # Handle case where they don't set a list_name in config
                    if not config_name:
                        self.list_id = row["id"]   
                        return self.list_id

                    # NOTE: Making case insensitive to avoid issues
                    if row["name"].lower() == config_name.lower():
                        self.list_id = row["id"]
                        return self.list_id
        return self.list_id

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""

        if response.status_code == 400:
            raise InvalidPayloadError(response.text or response)

        if response.status_code == 401 or response.status_code == 403:
            raise InvalidCredentialsError(response.text or response)

        # Potential timeout error, will retry
        if response.status_code == 502:
            raise RetriableAPIError(response.text or response)

        super().validate_response(response)

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
        self.list_id = self.get_list_id()

    
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
            return input
        
    def handle_custom_fields(self, client, record_custom_fields, merge_fields):
        #Check and populate custom fields as merge fields
        if self.custom_fields is None:
            try:
                _merge_fields = client.lists.get_list_merge_fields(self.list_id)
            except ApiClientError as error:
                handle_call_api_error(self.logger, error)
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
                try:
                    merge_field_res = client.lists.add_list_merge_field(self.list_id,{"name":field_name,"type":"text"})
                except ApiClientError as error:
                    handle_call_api_error(self.logger, error)
                self.custom_fields.update({merge_field_res["name"]: merge_field_res["tag"]})
        return merge_fields

    def process_batch_record(self, record: dict, index: int) -> dict:
        if self.stream_name.lower() in self.contact_names and self.config.get("process_batch_contacts", True):
            # Email is required, Mailchimp calls it email_address, unified schema calls it email
            email = get_email_if_exists(record)

            if not email:
                return({"error":"Email was not provided and it's a required value", "externalId": record.get("externalId"), "error_code": "HG_EMAIL_REQUIRED"})

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
            
            address = None
            addresses = record.get("addresses")
            if addresses:
                # sometimes it comes as a dict
                if not isinstance(addresses, list):
                    return({"error":"Addresses field is not formatted correctly, addresses format should be: [{'line1': 'xxxxx', 'city': 'xxxxx', 'state':'xxxxx', 'postal_code':'xxxxx'}]", "externalId": record.get("externalId"), "error_code": "HG_ADDRESS_FORMAT_ERROR"})
                
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
                        self.logger.info(f"Ignoring address due to empty or missing required fields: line1, city, zip, postal_code for record with email {email}")
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
                "email_address": email,
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
            client = MailchimpMarketing.Client()
            server = self.get_server()
            client.set_config(
                {"access_token": self.config.get("access_token"), "server": server}
            )
            
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
                        return({"error":f"Failed to post: List item '{list_name}' format is incorrect or incomplete, list item format should be 'groupTitle/groupName'", "externalId": record.get("externalId"), "error_code": "HG_LIST_ITEM_FORMAT_ERROR"})
                    
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
                        return({"error":f"Group title {group_title} not found in this account.", "externalId": record.get("externalId"), "error_code": "HG_GROUP_TITLE_NOT_FOUND"})                

                member_dict["interests"] = {group_name_id: True for group_name_id in group_name_ids}

            # clean null values
            member_dict = self.clean_convert(member_dict)
            if record.get("tags"):
                member_dict['tags'] = record.get('tags', [])

            # add email and externalid to externalid dict for state
            self.external_ids_dict[member_dict.get("email_address", "").lower()] = record.get("externalId", member_dict.get("email_address"))

            return member_dict
        
        else:
            # validate if email has been provided, it's a required field
            if not record.get("email_address"):
                return({"error":"Email was not provided and it's a required value", "externalId": record.get("externalId"), "error_code": "HG_EMAIL_REQUIRED"})
            
            if not record.get("status"):
                record["status_if_new"] = "subscribed"
            
            # validate address required fields
            if record.get("merge_fields").get("ADDRESS"):
                required_fields = ["addr1", "city", "state", "zip"]
                missing_fields = []
                address_fields = record.get("merge_fields").get("ADDRESS")
                for key in required_fields:
                    if key not in address_fields or not address_fields.get(key):
                        missing_fields.append(key)    
                if missing_fields:
                    return({"error":f"Missing required address fields: {','.join(missing_fields)}.", "externalId": record.get("externalId"), "error_code": "HG_ADDRESS_MISSING_FIELDS"})
            
            # get external id from record
            self.external_ids_dict[record["email_address"].lower()] = record.get("externalId", record["email_address"])
            return record

    def make_batch_request(self, records):
        if self.stream_name.lower() in self.contact_names:
            if self.list_id is not None:
                if records:
                    try:
                        client = MailchimpMarketing.Client()
                        client.set_config(
                            {
                                "access_token": self.config.get("access_token"),
                                "server": self.get_server(),
                            }
                        )

                        response = client.lists.batch_list_members(
                            self.list_id,
                            {"members": records, "update_existing": True},
                        )

                        return response
                    except ApiClientError as error:
                        custom_message_start = "Error: None of the records went through due to error "
                        custom_message_end = ", no state available."
                        handle_call_api_error(self.logger, error, custom_message_start, custom_message_end)
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
                "externalId": self.external_ids_dict.get(member.get("email_address", "").lower())
            })

        for error in response.get("errors", []):
            error_state_dict = {
                "success": False,
                "error": f"error: {error.get('error')}, field: {error.get('field')}, field_message: {error.get('value')}",
                "externalId": self.external_ids_dict.get(error.get("email_address", "").lower())
            }
            if classify_batch_error_or_false(error):
                error_state_dict.update(classify_batch_error_or_false(error))
            self.logger.info(f"Error state dict: {error_state_dict}")
            state_updates.append(error_state_dict)
        
        for map_error in map_errors:
            map_error_state_dict = {
                "success": False,
                "error": map_error.get("error"),
                "externalId": map_error.get("externalId")
            }
            if classify_batch_error_or_false(map_error):
                map_error_state_dict.update(classify_batch_error_or_false(map_error))

            self.logger.info(f"Error state dict: {map_error_state_dict}")

            state_updates.append(map_error_state_dict)
        return {"state_updates": state_updates}


    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        raw_records = context["records"]

        records = list(map(lambda e: self.process_batch_record(e[1], e[0]), enumerate(raw_records)))

        map_errors = [rec for rec in records if "error" in rec]
        records = [rec for rec in records if "error" not in rec]

        response = self.make_batch_request(records)

        result = self.handle_batch_response(response, map_errors)

        for state in result.get("state_updates", list()):
            self.update_state(state)


class FallbackSink(BaseSink, HotglueSink):
    """Precoro target sink class."""

    primary_key = "id"

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
        method = "POST"
        endpoint = self.endpoint
        if record:
            # send data
            # initialize server
            client = MailchimpMarketing.Client()
            server = self.get_server()
            client.set_config(
                {"access_token": self.config.get("access_token"), "server": server}
            )
            # custom logic for contacts
            if self.stream_name.lower() in self.contact_names:
                # add email to the endpoint to use create or update endpoint
                email = get_email_if_exists(record)

                if not email:
                    raise InvalidPayloadError(
                        f"No email found for record {record}, email is a required field."
                    )
                # get list id
                list_id = self.get_list_id()
                if not list_id:
                    raise InvalidPayloadError(
                        f"No list id found to send record with email {email}"
                    )
                # using create or update endpoint for contacts
                method = "PUT"
                endpoint = f"/lists/{list_id}/members/{email}"

            # send data
            id = record.pop("id", None)
            if id:
                method = self.update_method if hasattr(self, "update_method") else "PUT"
                endpoint = f"{endpoint}/{id}"

            # send data
            client = MailchimpMarketing.ApiClient()
            client.set_config(
                {
                    "access_token": self.config.get("access_token"),
                    "server": self.get_server(),
                }
            )
            try:
                response = client.call_api(
                    resource_path=endpoint, method=method, body=record
                )
            except ApiClientError as e:
                handle_call_api_error(self.logger, e)
            id = response[self.primary_key]
            return id, True, state_updates


class CustomFieldsSink(FallbackSink):
    """Custom fields sink class."""

    @property
    def endpoint(self) -> str:
        list_id = self.get_list_id()
        return f"/lists/{list_id}/merge-fields"

    update_method = "PATCH"
    primary_key = "merge_id"