"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_google_drive.tap import Tapgoogle-drive

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
TestTapgoogle-drive = get_tap_test_class(
    tap_class=Tapgoogle-drive,
    config=SAMPLE_CONFIG,
)


# TODO: Create additional tests as appropriate for your tap.
