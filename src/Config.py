import configparser
import config_static

class Config:
    def __init__(self):
        self.parser = configparser.ConfigParser()
        self.config_dynamic = config_static.config_dynamic

    def update_config(self):
        with open(self.config_dynamic, 'w') as configfile:
            self.parser.write(configfile)

    def set_config(self, section, name, value):
        self.parser.read(self.config_dynamic)
        self.parser[section][name] = str(value)
        self.update_config()

    def get_config(self, section, name):
        self.parser.read(self.config_dynamic)
        return self.parser[section][name]
