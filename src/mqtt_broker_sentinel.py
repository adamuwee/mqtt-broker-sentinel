import logger
import argparse
import config
import mqtt_subscriber
import process_monitor
import mqtt_topic_tracker
'''

'''
class MqttBrokerSentinel:

    '''
    Class Initialization - locals and setup; fast and no fail.
    '''
    def __init__(self,
                 app_logger : logger.Logger,
                 app_config : config.ConfigManager) -> None:
        self._app_logger = app_logger
        self._app_config = app_config

    '''
    Start the Sentinel thread. Non-blocking.
    '''
    def start(self) -> bool:
        start_ok = False
        # Initialize Sentinel - create connections, etc.
        self._app_logger.write("sentinel", "Starting...", logger.MessageLevel.INFO)

        # Start process monitor thread
        self._process_monitor = process_monitor.ProcessMonitor(self._app_config, 
                                                               self._app_logger,
                                                               self._process_monitor_tick_callback)
        self._process_monitor.start()

        # Topic Tracker - call before client is created
        self._topic_tracker = mqtt_topic_tracker.MqttTopicTracker(self._app_config, 
                                                                  self._app_logger)

        # Mqtt Client
        topic_base = "#"
        self._mqtt_client = mqtt_subscriber.MqttSubscriber(self._app_config, 
                                                     self._app_logger, 
                                                     self._new_mqtt_message_callback, 
                                                     topic_base)
        self._mqtt_client.start()


        self._app_logger.write("sentinel", "Started.", logger.MessageLevel.INFO)
        return start_ok
                               
    
    '''
    Stop the Sentinel thread. Blocking.
    '''
    def stop(self):
        # Stop the sentinel - close connections, etc.
        self._app_logger.write("mqtt-broker-sentinel", "Stopping...", logger.MessageLevel.INFO)
        self._process_monitor.stop()
        self._mqtt_client.stop()
        self._app_logger.write("mqtt-broker-sentinel", "Stopped.", logger.MessageLevel.INFO)
    
    '''
    Callback for every new message received
    '''
    def _new_mqtt_message_callback(self, topic, message):
        is_new_topic = self._topic_tracker.new_topic_data_received(topic)

    '''
    Callback from process monitor that checks every minute (default)
    '''
    def _process_monitor_tick_callback(self, process_exists : bool):

        # Once per second, print out the topic list
        topic_list = self._topic_tracker.get_copy_topic_list_with_deltas()
        topic_count = len(topic_list)
        self._app_logger.write("sentinel", f"Topics: {topic_count}", logger.MessageLevel.INFO)
        for (topic, (last_time, delta)) in topic_list.items():
            self._app_logger.write("sentinel", f'{topic:<70} {str(last_time):<30} {delta}', logger.MessageLevel.INFO) 

        # Print the topics in violation of the watchdog
        self._app_logger.write("sentinel", "<------------ Watchdog Violations ------------>", logger.MessageLevel.INFO)    
        for (topic, delta) in self._topic_tracker.get_topics_in_time_violation().items():
            self._app_logger.write("sentinel", f"Topic in violation: {topic} - {delta}", logger.MessageLevel.WARN) 
          
'''
Service Entry Point
'''
if __name__ == '__main__':
    
    # Initialize the logger
    app_logger = logger.Logger()
    app_logger.write("sentinel", "Initializing...", logger.MessageLevel.INFO)

    # Parse Arguments - https://docs.python.org/3/library/argparse.html
    arg_parser = argparse.ArgumentParser(
                        prog='MQTT Broker Sentinel',
                        description='Monitors a MQTT broker service and topic watchdog.',
                        epilog='Go forth and monitor.')
    # App argument list
    arg_parser.add_argument('-c', '--config', action='store_true',default=None)       # Specify a config file
    arg_parser.add_argument('-d', '--default', action='store_true',default=None)       # Use default config

    # Parse the arguments
    args = arg_parser.parse_args()
    for arg in vars(args):
        print(f"{arg} = {getattr(args, arg)}")
        # TODO: Handle spec'd config and default config

    # Create or load app config
    app_config = config.ConfigManager("mqtt-broker-sentinel.json", app_logger)

    # Create the sentinel and start it
    sentinel = MqttBrokerSentinel(app_logger, app_config)
    sentinel.start()

    # TODO: Capture key input or SIGINT to stop the sentinel