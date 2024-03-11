import logger
import time
import config
import paho.mqtt.client as mqtt
import random

# Multithreading Support
import threading
from queue import Queue

class MqttPublisher:

    # Private Class Constants
    _log_key = "mqtt_pub"
    
    # Private Class Members
    _logger = None

    _app_config = None
    _mqtt_client = None
    _process_command_queue = None
    _update_callback = None


    '''
    MQTT Publisher with callback support.
    '''
    def __init__(self, app_config : config.ConfigManager, app_logger) -> None:
        
        # Locals
        self._logger = app_logger
        self._app_config = app_config

        self._logger.write(self._log_key, "Initializing...", logger.MessageLevel.INFO)
        
        # Data Processing Queues
        self._market_data_queue = Queue()
        self._process_command_queue = Queue()

        # Build and start processing thread
        self._data_processing_thread = threading.Thread(target=self._data_processing_thread)

        # Init Done
        self._logger.write(self._log_key, "Init complete.", logger.MessageLevel.INFO)

    '''
    Start processing thread
    '''
    def start(self) -> None:
        if (not self._process_command_queue.empty()):
            self._logger.write(self._log_key, "Data queue is populated before start.", logger.MessageLevel.WARN)
        self._logger.write(self._log_key, "Starting...", logger.MessageLevel.INFO)
        client_id = f'python-mqtt-{random.randint(0, 1000)}'
        self._mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id)
        self._mqtt_start()
        self._data_processing_thread.start()
        self._logger.write(self._log_key, "Started.")
    '''
    Safely shutdown all of the model objects i.e. stop pushing data through the translation pipeline.
    '''
    def stop(self) -> None:
        self._logger.write(self._log_key, "Stopping...", logger.MessageLevel.INFO)
        self._mqtt_stop()
        self._process_command_queue.put('stop')
        
    '''
    Initialize the connection to the MQTT broker
    '''
    def _mqtt_start(self) -> tuple:
        broker_addr = self._app_config.active_config['mqtt_broker']['connection']['host_addr']
        broker_port = self._app_config.active_config['mqtt_broker']['connection']['host_port']
        connect_value = self._mqtt_client.connect(broker_addr, broker_port, 60)
        self._mqtt_client.on_connect = self._on_connect_callback
        self._mqtt_client.on_publish = self._on_publish_callback
        loop_start_value = self._mqtt_client.loop_start()
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

    def _mqtt_publish(self, topic, payload) -> mqtt.MQTTMessageInfo:
        full_topic = f"{self._app_config.active_config['mqtt']['topic_base']}{topic}"
        return self._mqtt_client.publish(full_topic, payload)
    
    def _on_connect_callback(self, client, userdata, flags, rc) -> None:
        self._logger.write(self._log_key, f"Connected with result code {rc}", logger.MessageLevel.INFO)

    def _on_publish_callback(self, client, userdata, mid) -> None:  
        self._logger.write(self._log_key, f"Published message ID: {mid}", logger.MessageLevel.INFO)
    '''
    Primary data processing thread after receiving market data and performing calculations (analytics).
    '''
    def _data_processing_thread(self)  -> None:

        # Locals
        model_perf = perf_mon.IntervalTime("Model Perf")
        empty_counter = 0
        run = True

        # Thread loop to process command and data queues. Limited by time.sleep to prevent CPU thrashing.
        # Hit the run flag via command queue to stop the thread.
        while run:
            # Command queue - process any commands, 'stop' is the only command supported at this time.
            if (not self._process_command_queue.empty()):
                command = self._process_command_queue.get()
                if (command == 'stop'):
                    run = False
                    break

            # Data queue - process market data from the symbol source *e.g. PolygonIO or Replay
            # Update the model performance
            if (self._market_data_queue.empty()):
                empty_counter += 1
            else:
                model_perf.tick()
                # Process the market data until the queue is empty
                # TODO: Need to filter out out-of-frame data e.g. data received before the current measure. 
                ## This should only happen if there is a major queue delay or the symbol source provides old data.
                while self._market_data_queue.empty() == False:
                    market_data_set = self._market_data_queue.get()      
                    self._mqtt_publish("market_data", market_data_set.to_json())

                # Print model perf stats
                if (model_perf.report_time_elapsed()):
                    self._logger.write(self._log_key, model_perf.report_stats(True), logger.MessageLevel.INFO)
                    self._logger.write(self._log_key, "Empty queue count: {0}".format(empty_counter), logger.MessageLevel.INFO)
                    empty_counter = 0
                    model_perf.reset()
            time.sleep(0.050) # 50ms sleep to prevent CPU thrashing
        self._logger.write(self._log_key, "Data processing thread exit.", logger.MessageLevel.INFO)


    