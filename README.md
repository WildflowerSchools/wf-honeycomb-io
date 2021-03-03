# honeycomb_io

Tools for working with data in Wildflower's Honeycomb database

## Installation

`pip install wf-honeycomb-io`

## Task list
* Integrate Honeycomb IO functions from `wf-camera-calibration`
* Integrate Honeycomb IO functions from `wf-video-io`
* Integrate Honeycomb IO functions from `wf-inference-helpers`
* Integrate Honeycomb IO functions from `cuwb_sensor`
* Integrate Honeycomb IO functions from `wf-inference-airflow`?
* Integrate Honeycomb IO functions from `wf-video-stream-consumer`?
* Integrate Honeycomb IO functions from `wf-pose-producer`?
* Integrate Honeycomb IO functions from `honeycomb-geom-processor`?
* Integrate Honeycomb IO functions from `wildflower-classroom-setup`?
* Integrate Honeycomb IO functions from `honeycomb-video-streamer`?
* Integrate Honeycomb IO functions from `wf-honeycomb-service-event-producer`?
* Integrate Honeycomb IO functions from `wf-process-ble-data`?
* Integrate Honeycomb IO functions from `classroom-video-uploader`?
* Integrate Honeycomb IO functions from `sensors_smcworker`?
* Redesign architecture so user doesn't need to create a client
* Create a function for each basic verb:
  - objects()
  - get_object()
  - find_objects()
  - search_objects()
  - create_object[s]()
  - update_object[s]()
  - delete_object[s]()
* Create corresponding functions for each verb/object combination?
* Rewrite existing external functions to use the basic functions above
* Consider replacing existing external functions with the basic functions above (changes API)
