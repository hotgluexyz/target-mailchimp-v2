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
                    if row["name"] == config_name:
                        self.list_id = row["id"]

            print(response)
        except ApiClientError as error:
            print("Error: {}".format(error.text))

    def process_record(self, record: dict, context: dict) -> None:
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
        if "addresses" in record:
            if len(record["addresses"]) > 0:
                location.update(
                    {
                        "country_code": record["addresses"][0]["country"],
                        "region": record["addresses"][0]["state"],
                    }
                )
        subscribed_status = self.config.get("subscribe_status", "subscribed")

        # override status if it is found in the record
        if record.get("subscribe_status"):
            subscribed_status = record.get("subscribe_status")

        self.all_members.append(
            {
                "email_address": record["email"],
                "status": subscribed_status,
                "merge_fields": {"FNAME": first_name, "LNAME": last_name},
                "location": location,
            }
        )

    def process_batch(self, context: dict) -> None:
        if self.list_id is not None:
            try:
                client = MailchimpMarketing.Client()
                client.set_config(
                    {
                        "access_token": self.config.get("access_token"),
                        "server": self.get_server(),
                    }
                )

                response = client.lists.batch_list_members(
                    self.list_id, {"members": self.all_members, "update_existing": True}
                )
                print(response)
                if response.get("error_count") > 0:
                    raise Exception(response.get("errors"))
            except ApiClientError as error:
                print("Error: {}".format(error.text))
