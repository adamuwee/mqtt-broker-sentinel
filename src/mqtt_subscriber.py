import paho.mqtt.client as mqtt
import logger
import config
import random

class MqttSubscriber:

    # Private Class Constants
    _log_key = "mqtt_sub"
    
    # Private Class Members
    _logger = None
    _app_config = None
    _mqtt_client = None
    _mqtt_topic = None

    '''
    MQTT Subscriber with callback support.
    Initialize config, logger, and callback.
    '''
    def __init__(self, 
                 app_config : config.ConfigManager, 
                 app_logger : logger.Logger, 
                 new_message_callback, 
                 mqtt_topic : str) -> None:
        
        # Locals
        self._logger = app_logger
        self._app_config = app_config
        self._new_message_callback = new_message_callback
        self._mqtt_topic = mqtt_topic

        self._logger.write(self._log_key, "Initializing...", logger.MessageLevel.INFO)

        # MQTT Connection
        client_id = f'python-mqtt-{random.randint(0, 1000)}'
        self._mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id)

        # Init Done
        self._logger.write(self._log_key, "Init complete.", logger.MessageLevel.INFO)

    '''
    Start mqtt thread
    '''
    def start(self) -> None:
        self._logger.write(self._log_key, "Starting...", logger.MessageLevel.INFO)
        (connect_value, loop_start_value) = self._mqtt_start()
        self._logger.write(self._log_key, f"Connected = {connect_value}.\tLoop Started = {loop_start_value}.")
        self._logger.write(self._log_key, "Started.")
    '''
    Safely shutdown all of the model objects i.e. stop pushing data through the translation pipeline.
    '''
    def stop(self) -> None:
        self._logger.write(self._log_key, "Stopping...", logger.MessageLevel.INFO)
        self._mqtt_stop()
        self._logger.write(self._log_key, "Stopped", logger.MessageLevel.INFO)
        
        
    '''
    Initialize the connection to the MQTT broker
    '''
    def _mqtt_start(self) -> tuple:
        broker_addr = self._app_config.active_config['mqtt_broker']['connection']['host_addr']
        broker_port = self._app_config.active_config['mqtt_broker']['connection']['host_port']
        connect_value = self._mqtt_client.connect(broker_addr, broker_port, 60)
        loop_start_value = self._mqtt_client.loop_start()
        self._mqtt_client.subscribe(self._mqtt_topic)
        self._mqtt_client.on_message = self._on_message_callback
        self._logger.write(self._log_key, f"ADDR={broker_addr}, PORT={broker_port}, CONNECTED={connect_value}", logger.MessageLevel.INFO)
        return (connect_value, loop_start_value)

    '''
    Stop the connection to the MQTT broker
    '''
    def _mqtt_stop(self) -> int:
        if self._mqtt_client is not None:
            self._mqtt_client.loop_stop()
            return self._mqtt_client.disconnect()
        return 0

    '''
    Internal callback for new messages received on the subscribed topic
    '''               
    def _on_message_callback(self, client, userdata, message) -> None:
        if (self._new_message_callback is not None):
            self._new_message_callback(message.topic, message.payload)