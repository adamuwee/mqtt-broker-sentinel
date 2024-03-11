import threading
import subprocess
import time
import config
import logger
import platform
import subprocess

class ProcessMonitor(threading.Thread):
    '''
    Initilize the ProcessMonitor with a process name
    '''
    def __init__(self, 
                 app_config : config.ConfigManager, 
                 app_logger : logger.Logger, 
                 process_monitor_tick_callback) -> None:

        # Locals
        self._logger = app_logger
        self._app_config = app_config
        self._process_monitor_tick_callback = process_monitor_tick_callback

        # Create thread
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
    '''
    Run the process monitor
    '''
    def run(self):
        while not self.stop_event.is_set():
            # Locals
            proc_name = self._app_config.active_config['mqtt_broker']['process']["name"]
            sleep_time_secs = self._app_config.active_config['mqtt_broker']['process']["service_wd_period_seconds"]
            process_exists = False
            if platform.system() == 'Windows':
                process_exists = self._windows_process_exists(proc_name)
                self._logger.write("proc_mon", f'Service {proc_name} running = {process_exists}. Sleeping for {sleep_time_secs} seconds.')
            elif platform.system() == 'Linux':
                result = subprocess.run(['systemctl', 'is-active', proc_name], stdout=subprocess.PIPE)
                self._logger.write("proc_mon", f'Service {proc_name} is {result.stdout.decode().strip()}. Sleeping for {sleep_time_secs} seconds.')
            else:
                self._logger.write("proc_mon", "Unknown OS - not checking any process. Sleeping for 60 seconds.")

            # Call the callback
            if self._process_monitor_tick_callback is not None:
                self._process_monitor_tick_callback(process_exists)
            time.sleep(sleep_time_secs)

    '''
    Stop the process monitor
    '''
    def stop(self):
        self.stop_event.set()

    '''
    Windows function to check if a process exists
    '''
    def _windows_process_exists(self, process_name) -> bool:
        call = 'TASKLIST', '/FI', 'imagename eq %s' % process_name
        # use buildin check_output right away
        output = subprocess.check_output(call).decode()
        # check in last line for process name
        last_line = output.strip().split('\r\n')[-1]
        # because Fail message could be translated
        return last_line.lower().startswith(process_name.lower())
