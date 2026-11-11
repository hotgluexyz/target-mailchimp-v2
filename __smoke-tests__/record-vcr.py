"""Colocated smoke-test runner for target-mailchimp-v2."""

import vcr
from hotglue_smoke_test.vcr.target import VCRTargetTestRunner


class TargetMailChimpV2TestRunner(VCRTargetTestRunner):
    # Mailchimp returns the audience ID in /lists, then uses it in later paths.
    PRESERVE_KEYS = {
        "dc",
        "id",
        "list_id",
        "rel"
    }

    def module(self) -> str:
        return "target_mailchimp_v2"

    def launch(self):
        from target_mailchimp_v2.target import TargetMailChimpV2

        TargetMailChimpV2.cli()

    def vcr_use_cassette(self, filter_query_parameters):
        return vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            filter_headers=list(self.FILTER_HEADERS),
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            match_on=["method", "scheme", "host", "port", "path", "query", "body"],
            before_record_response=self.before_record_response,
        )


if __name__ == "__main__":
    TargetMailChimpV2TestRunner.main()
