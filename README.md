# target-mailchimp-v2

`target-mailchimp-v2` is a Singer target for MailChimp-V2.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

## Installation

```bash
pipx install target-mailchimp-v2
```

## Configuration

### Accepted Config Options

| Setting | Required | Default | Description |
|---------|----------|---------|-------------|
| `access_token` | Yes | - | OAuth access token or Mailchimp API key (format: `key-datacenter`, e.g., `abc123-us22`) |
| `list_name` | No | - | Name of the Mailchimp list/audience to sync contacts to. If not set, the first list found is used. |
| `subscribe_status` | No | `"subscribed"` | Default subscription status for contacts. Valid values: `subscribed`, `unsubscribed`, `cleaned`, `pending`, `transactional`. |
| `process_batch_contacts` | No | `true` | When `true`, contact records are transformed from a unified schema into Mailchimp format. When `false`, records are passed through as-is and the fallback sink is used. |
| `use_fallback_sink` | No | `false` | When `true`, forces use of the FallbackSink (single record processing) instead of MailChimpV2Sink (batch processing) for contact streams. |

### MailChimpV2Sink Behavior

The `MailChimpV2Sink` handles batch processing for contact-related streams (`customers`, `contacts`, `customer`, `contact`, `list_members`).

#### Flag Behaviors

- **`process_batch_contacts: true` (default)**
  - Transforms incoming records from a unified schema into Mailchimp member format
  - Automatically maps `name` → `FNAME`/`LNAME` merge fields
  - Handles `addresses` array → Mailchimp `ADDRESS` merge field
  - Handles `phone_numbers` array → `PHONE` merge field
  - Processes `custom_fields` array and creates merge fields if they don't exist
  - Supports `lists` field for group/interest category assignments (format: `"groupTitle/groupName"`)
  - Supports `tags` field for contact tagging
  - Uses `subscribe_status` from config or record-level override

- **`process_batch_contacts: false`**
  - Records are passed through without transformation
  - Expects records to already be in Mailchimp's native format
  - Uses the FallbackSink for single-record processing

- **`use_fallback_sink: true`**
  - Bypasses batch processing entirely
  - Uses PUT requests to the `/lists/{list_id}/members/{email}` endpoint
  - Useful for handling edge cases or when batch processing is not desired

#### Record-Level Overrides

Individual records can override global config settings:

- `subscribe_status`: Set on a record to override the global `subscribe_status` config

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-mailchimp-v2 --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Authentication

The target supports two authentication methods:

1. **OAuth Access Token**: Obtained through Mailchimp's OAuth 2.0 flow. The target will automatically fetch the datacenter from Mailchimp's metadata endpoint.

2. **API Key**: A Mailchimp API key in the format `key-datacenter` (e.g., `abc123-us22`). The datacenter is extracted directly from the key.

Example config with API key:
```json
{
  "access_token": "your-api-key-us22",
  "list_name": "My Newsletter"
}
```

## Usage

You can easily run `target-mailchimp-v2` by itself or in a pipeline using.

### Executing the Target Directly

```bash
target-mailchimp-v2 --version
target-mailchimp-v2 --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-mailchimp-v2 --config /path/to/target-mailchimp-v2-config.json
```

## Developer Resources

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_mailchimp_v2/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-mailchimp-v2` CLI interface directly using `poetry run`:

```bash
poetry run target-mailchimp-v2 --help
```