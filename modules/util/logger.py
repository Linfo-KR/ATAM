import json
import logging

from logging.handlers import TimedRotatingFileHandler

class Logger:
    def __init__(self, config='./config/log/logging_configs.json'):
        with open(config, 'r') as config:
            self.config = json.load(config)
            
        self.loggers = {}
        self.setup_loggers()
        
    def setup_loggers(self):
        for module, settings in self.config.items():
            logger = logging.getLogger(module)
            logger.setLevel(self.get_log_level(settings['level']))
            
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
            
            if 'file' in settings:
                file_handler = TimedRotatingFileHandler(
                    filename=settings['file'],
                    when=settings.get('rotate_when', 'midnight'),
                    interval=settings.get('rotate_interval', 1),
                    backupCount=settings.get('backup_count', 7)
                )
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)
            
            self.loggers[module] = logger
            
    def get_logger(self, module_name):
        return self.loggers.get(module_name, logging.getLogger(module_name))
    
    @staticmethod
    def get_log_level(level):
        return getattr(logging, level.upper(), logging.INFO)
    
    def update_log_level(self, module_name, new_level):
        if module_name in self.loggers:
            self.loggers[module_name].setLevel(self.get_log_level(new_level))
            self.config[module_name]['level'] = new_level
            self.save_config()
            
    def update_log_file(self, module_name, new_file):
        if module_name in self.loggers:
            logger = self.loggers[module_name]
            for handler in logger.handlers:
                if isinstance(handler, TimedRotatingFileHandler):
                    logger.removeHandler(handler)
                    break
                
            new_handler = TimedRotatingFileHandler(
                filename = new_file,
                when = self.config[module_name].get('rotate_when', 'midnight'),
                interval = self.config[module_name].get('rotate_interval', 1),
                backupCount = self.config[module_name].get('backup_count', 7)
            )
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            new_handler.setFormatter(formatter)
            logger.addHandler(new_handler)
            
            self.config[module_name]['file'] = new_file
            self.save_config()
            
    def save_config(self):
        with open('./config/log/logging_configs.json', 'w') as config:
            json.dump(self.config, config, indent=4)