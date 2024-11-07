"""MailChimp-V2 target class."""

from singer_sdk import typing as th
from singer_sdk.target_base import Target
from target_hotglue.target import TargetHotglue
from target_hotglue.target_base import update_state
import copy
from singer_sdk.sinks import BatchSink
from singer_sdk.helpers._compat import final

from target_mailchimp_v2.sinks import BaseSink, MailChimpV2Sink, FallbackSink


class TargetMailChimpV2(TargetHotglue):
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
    SINK_TYPES = []

    def get_sink_class(self, stream_name: str):
        """Get sink for a stream."""
        if stream_name.lower() in BaseSink.contact_names and not self.config.get("use_fallback_sink"):
            return MailChimpV2Sink
        return FallbackSink

    @final
    def drain_all(self, is_endofpipe: bool = False) -> None:
        """Drains all sinks, starting with those cleared due to changed schema.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            is_endofpipe: This is passed by the
                          :meth:`~singer_sdk.Sink._process_endofpipe()` which
                          is called after the target instance has finished
                          listening to the stdin
        """
        state = copy.deepcopy(self._latest_state)
        self._drain_all(self._sinks_to_clear, 1)
        if is_endofpipe:
            for sink in self._sinks_to_clear:
                if sink:
                    sink.clean_up()
        self._sinks_to_clear = []
        self._drain_all(list(self._sinks_active.values()), self.max_parallelism)
        if is_endofpipe:
            for sink in self._sinks_active.values():
                if sink:
                    sink.clean_up()

        # Build state from BatchSinks
        batch_state = dict()
        batch_sinks = [
            s for s in self._sinks_active.values() if isinstance(s, BatchSink)
        ]
        for s in batch_sinks:
            # batch_state = update_state(batch_state, s.latest_state, self.logger)
            state = update_state(batch_state, s.latest_state, self.logger)

        # # If there was at least one BatchSink and if "batch_state" isn't a empty dict,
        # # update the current "state"
        # if batch_sinks and batch_state:
        #     state = update_state(state, batch_state, self.logger)

        self._write_state_message(state)
        self._reset_max_record_age()


if __name__ == "__main__":
    TargetMailChimpV2.cli()
