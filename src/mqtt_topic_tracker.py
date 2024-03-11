import datetime
import copy
import config
import logger

'''
Keeps a list of topics and notifies when a new topic is received. It also monitors the last time a topic was received.
'''
class MqttTopicTracker:

    # Private Members
    _log_key = 'topic_tracker'

    '''
    Initialize the tracker. Fast, no fail.
    '''
    def __init__(self, 
                 app_config : config.ConfigManager, 
                 app_logger : logger.Logger) -> None:

        # Locals
        self._logger = app_logger
        self._app_config = app_config
        self._topics = dict()

    '''
    Called when a new topic is received by the MQTT Client
    '''
    def new_topic_data_received(self, topic) -> bool:
        is_new_topic = (self._topics.get(topic, None) == None)
        self._topics[topic] = datetime.datetime.now()
        if is_new_topic:
            self._logger.write(self._log_key, f"New topic received: {topic}", logger.MessageLevel.INFO)
        return is_new_topic

    '''
    Returns a copy of the topics and last reported datetime
    '''
    def get_copy_topic_list(self):
        return copy.deepcopy(self._topics)
    
    '''
    Returns a copy of the topics and last reported datetime with the time deltas
    '''
    def get_copy_topic_list_with_deltas(self):
        now = datetime.datetime.now()
        topic_list_with_deltas = dict()
        for (topic, last_time) in self.get_copy_topic_list().items():
            delta = now - last_time
            topic_list_with_deltas[topic] = (last_time, delta)
        return topic_list_with_deltas
    
    '''
    Returns a list of topics that have not been received in the specified time (config)
    '''
    def get_topics_in_time_violation(self) -> list:
        topic_list = dict()
        watchdog_list = self._get_watchdog_list()
        all_topics_max_time_seconds = self._app_config.active_config['topic_watchdog']['all']['max_time_seconds']
        for (topic, (last_time, delta)) in self.get_copy_topic_list_with_deltas().items():
            if (delta.total_seconds() > all_topics_max_time_seconds):
                topic_list[topic] = (last_time, delta)
            else:
                for (wdt_topic, watchdog_time_seconds) in watchdog_list.items():
                    if (topic == wdt_topic and delta.total_seconds() > watchdog_time_seconds):
                        topic_list[topic] = (last_time, delta)
        return topic_list
    
    '''
    Gets a dict of the watchdog list and excludes 'all'
    '''
    def _get_watchdog_list(self) -> dict:
        topics = self._app_config.active_config['topic_watchdog']
        watchdog_list = dict()
        for (topic, watchdog_time_seconds) in self._app_config.active_config['topic_watchdog'].items():
            if topic != 'all':
                watchdog_list[topic] = watchdog_time_seconds['max_time_seconds']
        return watchdog_list