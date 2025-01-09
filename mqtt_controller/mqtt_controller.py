from typing import Dict, Any
import json
import paho.mqtt.client as mqtt
from login_credentials import *


class MQTTController:

    def __init__(self, mqtt_client_id: str):
        self.mqtt_connected = False
        self.client = mqtt.Client(client_id=mqtt_client_id)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.packet_counter = 0

        try:
            print("Authenticating with user:", mqtt_username, "on MQTT connection")
            self.client.username_pw_set(mqtt_username, mqtt_password)
        except AttributeError:
            print("Using no authentication on MQTT connection")

        # if config.MQTT_USE_TLS:
        #     print("using TLS for MQTT Connection")
        #     self.client.tls_set()

        try:
            self.client.connect(mqtt_server, mqtt_port)
        except Exception as e:
            print(f"Can't connect to MQTT Broker:{mqtt_server} at port:{mqtt_port}, dump: {e}")

        self.client.loop_start()  # Start MQTT handling in a new thread

    def _get_next_packet_count(self) -> int:
        self.packet_counter += 1
        return self.packet_counter

    def get_connected(self) -> bool:
        return self.mqtt_connected

    def _on_connect(self, _client, _userdata, _flags, _rc) -> None:
        print("Connected to MQTT Broker:", mqtt_server, "at port:", mqtt_port)
        self.mqtt_connected = True

    def _on_disconnect(self, _client, _userdata, _rc) -> None:
        print("Disconnected from MQTT Broker:", mqtt_server, "at port:", mqtt_port)
        self.mqtt_connected = False

    def publish_data(self, data: Dict[str, Any]) -> None:
        data["tele"]["packet_count"] = self._get_next_packet_count()
        json_data = json.dumps(data, indent=4)
        self.client.publish(mqtt_topic + "/" + mqtt_client_id, json_data, qos=2)
        #print("mqtt publish: ", data)

    def stop(self) -> None:
        self.client.loop_stop()
