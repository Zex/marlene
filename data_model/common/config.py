# Configure holder
# Author: Zex Li <top_zlynch@yahoo.com>
import os
import string

class Config(object):

    def __init__(self):
        self.load_config()

    def load_config(self):
        self.load_config_env()

    def load_config_env(self):
        {setattr(self, k.lower().lstrip(string.digits+string.punctuation), v) \
                for k, v in os.environ.items() if k}

    def raise_on_not_set(self, name):
        if not hasattr(self, name):
            raise AttributeError("{} not set".format(name.upper()))


def get_config():
    if not globals().get('config'):
        globals()['config'] = Config()
    return globals().get('config')
