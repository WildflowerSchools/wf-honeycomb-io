import datetime
import logging
import re

logger = logging.getLogger(__name__)

# Used by:
# video_io.core (wf-video-io)
def from_honeycomb_datetime(honeycomb_datetime):
    if honeycomb_datetime is None:
        return None
    return datetime.datetime.strptime(honeycomb_datetime, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=datetime.timezone.utc)

# Used by:
# video_io.core (wf-video-io)
def to_honeycomb_datetime(python_datetime):
    if python_datetime is None:
        return None
    return python_datetime.astimezone(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

# Used by:
# camera_calibration.colmap (wf-camera-calibration)
def extract_honeycomb_id(string):
    id = None
    m = re.search(
        '(?P<id>[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12})',
        string
    )
    if m:
        id = m.group('id')
    return id
