from functools import wraps


def singleton(cls):
    instances = {}

    def singleton_new(cls, *args, **kwargs):
        if cls not in instances:
            instances[cls] = cls.__origin_new__(cls, *args, **kwargs)

        return instances[cls]
    
    cls.__origin_new__ = cls.__new__
    cls.__new__ = singleton_new

    return cls
