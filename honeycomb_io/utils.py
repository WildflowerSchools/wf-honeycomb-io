import datetime
import logging

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
