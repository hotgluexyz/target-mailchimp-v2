import vcr

from hotglue_smoke_test.vcr.target import VCRTargetTestRunner


class TargetMailChimpV2TestRunner(VCRTargetTestRunner):
    def module(self) -> str:
        return "target_mailchimp_v2"

    def launch(self):
        from target_mailchimp_v2.target import TargetMailChimpV2

        TargetMailChimpV2.cli()

    def scrub_mailchimp_response_headers(self, response):
        headers_to_scrub = ["set-cookie"]
        headers = response.get("headers")
        if not isinstance(headers, dict):
            return response
        for key in list(headers.keys()):
            if key.lower() in headers_to_scrub:
                del headers[key]
        return response

    def vcr_use_cassette(self, filter_query_parameters):
        my_vcr = vcr.VCR()
        return my_vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            filter_headers=["authorization"],
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            match_on=["method", "scheme", "host", "port", "path", "query", "body"],
            before_record_response=self.scrub_mailchimp_response_headers,
        )


if __name__ == "__main__":
    TargetMailChimpV2TestRunner.main()
