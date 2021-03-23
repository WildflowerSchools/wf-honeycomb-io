import pandas as pd
import datetime
import logging
import re

logger = logging.getLogger(__name__)

def parse_data_sequence(data):
    if isinstance(data, dict):
        data_list = [data]
    elif isinstance(data, list):
        data_list = data
    elif isinstance(data, tuple):
        data_list = list(data)
    elif isinstance(data, pd.core.frame.DataFrame):
        if data.index.name is not None:
            data = data.reset_index(drop=False)
        data_list = data.to_dict(orient='records')
    else:
        raise ValueError('Data must be dict, list, tuple, or Pandas DataFrame')
    return data_list

def parse_data_id_sequence(ids):
    if isinstance(ids, str):
        data_id_list = [ids]
    elif isinstance(ids, list):
        data_id_list = ids
    elif isinstance(ids, tuple):
        data_id_list = list(ids)
    else:
        raise ValueError('IDs must be str, list, or tuple')
    return data_id_list

# Used by:
# video_io.core (wf-video-io)
def from_honeycomb_datetime(honeycomb_datetime):
    if honeycomb_datetime is None:
        return None
    return datetime.datetime.strptime(honeycomb_datetime, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=datetime.timezone.utc)

# Used by:
# video_io.core (wf-video-io)
def to_honeycomb_datetime(input_datetime):
    if input_datetime is None:
        return None
    return pd.to_datetime(input_datetime, utc=True).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

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

def minutes_elapsed(
    begin,
    end
):
    if pd.notnull(begin) and pd.notnull(end):
        begin = pd.to_datetime(begin, utc=True).to_pydatetime()
        end = pd.to_datetime(end, utc=True).to_pydatetime()
        minutes_elapsed = (end - begin).total_seconds()/60
        return minutes_elapsed
    return None
