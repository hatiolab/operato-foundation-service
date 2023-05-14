import json
import time


def print_elasped_time(wrappee):
    def wrapper_fn(*args, **kwargs):
        start_time = time.time()
        result = wrappee(*args, **kwargs)
        end_time = time.time()
        print(f"WorkingTime[{wrappee.__name__}]: {end_time - start_time} (sec)")
        return result

    return wrapper_fn


def convert_str_dict_to_bytearray(indata):
    return (
        indata.encode()
        if type(indata) == str
        else json.dumps(indata).encode()
        if type(indata) == dict
        else str(indata).encode()
    )


def convert_bytearray_to_dict(indata):
    return json.loads(indata.decode("utf-8"))

import sys 

def debugger_is_active() -> bool:
    """Return if the debugger is currently active"""
    return hasattr(sys, 'gettrace') and sys.gettrace() is not None
