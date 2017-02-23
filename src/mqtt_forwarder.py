import paho.mqtt.publish as publish
import json

def to_mqtt(event):
    pid, table, payload = event
    payload_obj = json.loads(payload)
    topic = payload_obj.get('table', 'events')

    publish.single(
        # TODO, better topics
        topic=topic,
        payload=payload,
        qos=0,
        # retain=False,
        # hostname="localhost",
        # port=1883,
        # client_id="",
        # keepalive=60,
        # will=None,
        # auth=None,
        # tls=None,
        #protocol=mqtt.MQTTv311
    )
