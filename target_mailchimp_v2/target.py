"""MailChimp-V2 target class."""

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_mailchimp_v2.sinks import MailChimpV2Sink


class TargetMailChimpV2(Target):
    name = "target-mailchimp-v2"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            description="access_token from oAuth",
            required=True,
        ),
        th.Property("list_name", th.StringType, required=False),
    ).to_dict()
    default_sink_class = MailChimpV2Sink


if __name__ == "__main__":
    TargetMailChimpV2.cli()
