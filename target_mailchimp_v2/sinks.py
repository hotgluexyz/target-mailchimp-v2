"""MailChimp-V2 target sink class, which handles writing streams."""


import mailchimp_marketing as MailchimpMarketing
import requests
from mailchimp_marketing.api_client import ApiClientError
from singer_sdk.sinks import BatchSink
from target_hotglue.client import HotglueBatchSink

class MailChimpV2Sink(HotglueBatchSink):
    max_size = 500  # Max records to write in one batch
    list_id = None
    server = None
    external_ids_dict = {}

    @property
    def name(self) -> str:
        return self.stream_name

    @property
    def endpoint(self) -> str:
        raise ""

    @property
    def base_url(self) -> str:
        return ""

    @property
    def unified_schema(self):
        return None

    groups_dict = None

    def get_server(self):
        if self.server is None:
            self.server = self.get_server_meta_data()

        return self.server

    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    def get_server_meta_data(self):
        header = {"Authorization": f"OAuth {self.config.get('access_token')}"}
        metadata = requests.get(
            "https://login.mailchimp.com/oauth2/metadata", headers=header
        ).json()
        
        if "error" in metadata:
            raise Exception(metadata["error"])
        
        return metadata["dc"]

    def start_batch(self, context: dict) -> None:
        try:
            client = MailchimpMarketing.Client()
            server = self.get_server()
            client.set_config(
                {"access_token": self.config.get("access_token"), "server": server}
            )

            response = client.lists.get_all_lists()
            config_name = self.config.get("list_name")
            if "lists" in response:
                for row in response["lists"]:
                    # Handle case where they don't set a list_name in config
                    if not config_name:
                        self.list_id = row["id"]
                        break

                    # NOTE: Making case insensitive to avoid issues
                    if row["name"].lower() == config_name.lower():
                        self.list_id = row["id"]

            self.logger.info(response)
        except ApiClientError as error:
            self.logger.exception("Error: {}".format(error.text))
    
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

    def process_batch_record(self, record: dict, index: int) -> dict:
        if self.stream_name.lower() in ["customers", "contacts", "customer", "contact"]:
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
            if not record.get("email"):
                return({"map_error":"Email was not provided and it's a required value", "externalId": record.get("externalId")})
            
            address = None
            addresses = record.get("addresses")
            if addresses:
                # sometimes it comes as a dict
                if not isinstance(addresses, list):
                    return({"map_error":"Addresses field is not formatted correctly, addresses format should be: [{'line1': 'xxxxx', 'city': 'xxxxx', 'state':'xxxxx', 'postal_code':'xxxxx'}]", "externalId": record.get("externalId")})
                
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
                "email_address": record["email"],
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
            self.external_ids_dict[record["email"]] = record.get("externalId", record["email"])

            # add groups 
            lists = record.get("lists")
            if record.get("lists"):
                group_names = {}
                # initialize server
                client = MailchimpMarketing.Client()
                server = self.get_server()
                client.set_config(
                    {"access_token": self.config.get("access_token"), "server": server}
                )
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
                        return({"map_error":f"Failed to post: List item '{list_name}' format is incorrect or incomplete, list item format should be 'groupTitle/groupName'", "externalId": record.get("externalId")})
                    
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
                        return({"map_error":f"Group title {group_title} not found in this account.", "externalId": record.get("externalId")})                

                member_dict["interests"] = {group_name_id: True for group_name_id in group_name_ids}

            # clean null values
            member_dict = self.clean_convert(member_dict)
            return member_dict

    def make_batch_request(self, records):
        if self.stream_name.lower() in ["customers", "contacts", "customer", "contact"]:
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
                "map_error": map_error.get("map_error"),
                "externalId": map_error.get("externalId")
            })

        return {"state_updates": state_updates}


    def process_batch(self, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        raw_records = context["records"]

        records = list(map(lambda e: self.process_batch_record(e[1], e[0]), enumerate(raw_records)))

        map_errors = [rec for rec in records if "map_error" in rec]
        records = [rec for rec in records if "map_error" not in rec]

        response = self.make_batch_request(records)

        result = self.handle_batch_response(response, map_errors)

        for state in result.get("state_updates", list()):
            self.update_state(state)