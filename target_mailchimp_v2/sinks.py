"""MailChimp-V2 target sink class, which handles writing streams."""


import mailchimp_marketing as MailchimpMarketing
import requests
from mailchimp_marketing.api_client import ApiClientError
from singer_sdk.sinks import BatchSink


class MailChimpV2Sink(BatchSink):
    max_size = 10000  # Max records to write in one batch
    list_id = None
    all_members = []
    server = None

    def get_server(self):
        if self.server is None:
            self.server = self.get_server_meta_data()

        return self.server

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

    def process_record(self, record: dict, context: dict) -> None:
        if self.stream_name.lower() in ["customers", "contacts", "customer", "contact"]:
            first_name, *last_name = record["name"].split()
            last_name = " ".join(last_name)
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
            # Iterate through all of the possible merge fields, if one is None
            # then it is removed from the dictionary
            keys_to_remove = []
            for field, value in merge_fields.items():
                if value == None:
                    keys_to_remove.append(field)
            
            for key in keys_to_remove:
                merge_fields.pop(key)

            member_dict["merge_fields"] = merge_fields
            self.all_members.append(member_dict)

    def process_batch(self, context: dict) -> None:
        if self.stream_name.lower() in ["customers", "contacts", "customer", "contact"]:
            if self.list_id is not None and len(self.all_members) > 0:
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
                        {"members": self.all_members, "update_existing": True},
                    )
                    self.logger.info(response)
                    if response.get("error_count") > 0:
                        raise Exception(response.get("errors"))
                except ApiClientError as error:
                    self.logger.exception("Error: {}".format(error.text))
            else:
                self.logger.error(
                    f"Failed to post because there was no list ID found for the list name {self.config.get('list_name')}!"
                )
