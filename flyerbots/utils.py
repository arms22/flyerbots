# -*- coding: utf-8 -*-
from functools import wraps
import time
import json
import os.path
import logging

logger = logging.getLogger(__name__)

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    def __getattr__(self, attr):
        return self.get(attr)
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

def stop_watch(func):
    @wraps(func)
    def wrapper(*args, **kargs):
        start = time.time()
        result = func(*args,**kargs)
        process_time =  (time.time() - start)
        units = 's'
        if process_time < 1:
            process_time *= 1000
            units = 'ms'
        if process_time < 1:
            process_time *= 1000
            units = 'us'
        print("Processing time for {0}:{1:.3f}{2}".format(func.__name__, process_time, units))
        return result
    return wrapper

class reloadable_jsondict(dotdict):
    def __init__(self, jsonfile, default_value={}):
        self.mtime = 0
        self.jsonfile = jsonfile
        self.update(default_value)
        self.reload()

    def reload(self):
        try:
            mtime = os.path.getmtime(self.jsonfile)
            if mtime > self.mtime:
                json_dict = json.load(open(self.jsonfile, 'r'), object_hook=dotdict)
                self.update(json_dict)
                self.mtime = mtime
                self.reloaded = True
        except Exception as e:
            logger.warning(type(e).__name__ + ": {0}".format(e))
        return self
