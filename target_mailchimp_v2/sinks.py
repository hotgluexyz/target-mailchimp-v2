"""MailChimp-V2 target sink class, which handles writing streams."""


import mailchimp_marketing as MailchimpMarketing
import requests
from mailchimp_marketing.api_client import ApiClientError
from singer_sdk.sinks import BatchSink
from target_hotglue.client import HotglueBatchSink

class MailChimpV2Sink(HotglueBatchSink):
    max_size = 10000  # Max records to write in one batch
    list_id = None
    server = None
    custom_fields = []

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
            address = None
            if "addresses" in record:
                if len(record["addresses"]) > 0:
                    address_dict = record["addresses"][0]
                    location.update(
                        {
                            "country_code": address_dict["country"],
                            "region": address_dict["state"],
                        }
                    )
                    address = {
                        "addr1": address_dict.get("line1"),
                        "city": address_dict.get("city"),
                        "state": address_dict.get("state"),
                        "zip": address_dict.get("postalCode"),
                        "country": address_dict.get("country")
                    }
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
                "ADDRESS": address
            }
            #Check and populate custom fields as merge fields    
            if "custom_fields" in record:
                if isinstance(record['custom_fields'],list):
                    for field in  record['custom_fields']:
                        if "name" in field:
                            merge_fields.update({ field['name']:field['value']})
                            if field['name'] not in self.custom_fields:
                                self.custom_fields.append(field['name']) 
            # Iterate through all of the possible merge fields, if one is None
            # then it is removed from the dictionary
            keys_to_remove = []
            for field, value in merge_fields.items():
                if value == None:
                    keys_to_remove.append(field)
            
            for key in keys_to_remove:
                merge_fields.pop(key)

            member_dict["merge_fields"] = merge_fields
            return member_dict
        
    def verify_add_merge_field(self,client):
        if self.custom_fields:
            merge_fields = client.lists.get_list_merge_fields(self.list_id)
            if merge_fields:
                merge_fields_list = []
                for merge_field in merge_fields['merge_fields']:
                    merge_fields_list.append(merge_field['name'])
            for custom_field in self.custom_fields:
                if custom_field not in merge_fields_list:
                    #Add merge field
                    client.lists.add_list_merge_field(self.list_id,{"name":custom_field,"type":"text"})

    def make_batch_request(self, records):
        if self.stream_name.lower() in ["customers", "contacts", "customer", "contact"]:
            if self.list_id is not None and len(records) > 0:
                try:
                    client = MailchimpMarketing.Client()
                    client.set_config(
                        {
                            "access_token": self.config.get("access_token"),
                            "server": self.get_server(),
                        }
                    )
                    #Check and add merge field to the list if required.
                    res = self.verify_add_merge_field(client)
                    response = client.lists.batch_list_members(
                        self.list_id,
                        {"members": records, "update_existing": True},
                    )

                    return response
                except ApiClientError as error:
                    self.logger.exception("Error: {}".format(error.text))
            else:
                raise Exception(
                    f"Failed to post because there was no list ID found for the list name {self.config.get('list_name')}!"
                )

    def handle_batch_response(self, response) -> dict:
        """
        This method should return a dict.
        It's recommended that you return a key named "state_updates".
        This key should be an array of all state updates
        """
        state_updates = []
        members = response.get("new_members") + response.get("updated_members")

        for member in members:
            state_updates.append({
                "success": True,
                "id": member["id"],
                "externalId": member.get("email_address")
            })

        for error in response.get("errors"):
            state_updates.append({
                "success": False,
                "error": error.get("error"),
                "externalId": error.get("email_address")
            })

        return {"state_updates": state_updates}
