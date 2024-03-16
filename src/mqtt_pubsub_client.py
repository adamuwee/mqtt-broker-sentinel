import random

import logger
import config
import paho.mqtt.client as mqtt

class MqttSubscriber:
    """MQTT Subscriber with callback support."""
    
    # Private Class Constants
    _log_key = "mqtt_pubsub"
    
    # Private Class Members
    _logger = None
    _app_config = None
    _mqtt_client = None
    _mqtt_topic = None

    def __init__(self, 
                 app_config : config.ConfigManager, 
                 app_logger : logger.Logger, 
                 new_message_callback, 
                 publish_message_callback,
                 mqtt_topic : str) -> None:
        '''MQTT Subscriber with callback support. Initialize config, logger, and callback.'''
        # Locals
        self._logger = app_logger

        self._logger.write(self._log_key, "Initializing...", logger.MessageLevel.INFO)
        self._app_config = app_config
        self._new_message_callback = new_message_callback
        self._mqtt_topic = mqtt_topic
        self._publish_message_callback = publish_message_callback

        # Init Done
        self._logger.write(self._log_key, "Init complete.", logger.MessageLevel.INFO)

    def start(self) -> None:
        '''Start the MQTT client and begin listening for messages on the subscribed topic.'''
        self._logger.write(self._log_key, "Starting...", logger.MessageLevel.INFO)
        client_id = f'python-mqtt-{random.randint(0, 1000)}'
        self._mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id)
        (connect_value, loop_start_value) = self._mqtt_start()
        self._logger.write(self._log_key, f"Connected = {connect_value}.\tLoop Started = {loop_start_value}.")
        self._logger.write(self._log_key, "Started.")
        
    def stop(self) -> None:
        '''Safely shutdown all of the model objects i.e. stop pushing data through the translation pipeline.'''
        self._logger.write(self._log_key, "Stopping...", logger.MessageLevel.INFO)
        self._mqtt_stop()
        self._logger.write(self._log_key, "Stopped", logger.MessageLevel.INFO)
    
    def is_connected(self) -> bool:
        '''Return true/false if the MQTT client is connected'''
        return self._mqtt_client.is_connected()
        
    def _mqtt_start(self) -> tuple:
        '''Internal function - Initialize the connection to the MQTT broker'''
        broker_addr = self._app_config.active_config['mqtt_broker']['connection']['host_addr']
        broker_port = self._app_config.active_config['mqtt_broker']['connection']['host_port']
        connect_value = self._mqtt_client.connect(broker_addr, broker_port, 60)
        loop_start_value = self._mqtt_client.loop_start()
        self._mqtt_client.subscribe(self._mqtt_topic)
        self._mqtt_client.on_message = self._on_message_callback
        self._mqtt_client.on_connect = self._on_connect_callback
        self._logger.write(self._log_key, f"ADDR={broker_addr}, PORT={broker_port}, CONNECTED={connect_value}", logger.MessageLevel.INFO)
        return (connect_value, loop_start_value)

    def _mqtt_stop(self) -> int:
        ''' Internal function - Disconnect from the MQTT broker and stop the loop.'''
        if self._mqtt_client is not None:
            self._mqtt_client.loop_stop()
            return self._mqtt_client.disconnect()
        return 0
    
    def mqtt_publish(self, topic, payload) -> mqtt.MQTTMessageInfo:
        '''Publish a payload to a given topic'''
        return self._mqtt_client.publish(topic, payload)
    
    def _on_connect_callback(self, client, userdata, flags, rc) -> None:
        '''Internal callback for a new connection to the MQTT broker'''
        self._logger.write(self._log_key, f"Connected with result code {rc}", logger.MessageLevel.INFO)

    def _on_publish_callback(self, client, userdata, mid) -> None:  
        '''Internal callback for a new message published to the MQTT broker'''
        self._logger.write(self._log_key, f"Published message ID: {mid}", logger.MessageLevel.INFO)
        if self._publish_message_callback is not None:
            self._publish_message_callback(mid)
             
    def _on_message_callback(self, client, userdata, message) -> None:
        '''Internal callback for new messages received on the subscribed topic'''
        if (self._new_message_callback is not None):
            self._new_message_callback(message.topic, message.payload)
    
    def _on_connect_callback(self, client, userdata, flags, rc) -> None:
        '''Internal callback for a new connection to the MQTT broker'''
        self._logger.write(self._log_key, f"Connected with result code {rc}", logger.MessageLevel.INFO)
        self._mqtt_client.subscribe(self._mqtt_topic)
        self._logger.write(self._log_key, f"Subscribed to {self._mqtt_topic}", logger.MessageLevel.INFO)