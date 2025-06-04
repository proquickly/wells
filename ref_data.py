from abc import ABC
from abc import abstractmethod
import functools
import time
import sqlite3


def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f"Ran {func.__name__!r} in {run_time:.4f} secs")
        return value

    return wrapper_timer


class Memo:
    def __init__(self, func):
        self.func = func
        self.memo = {}

    def __call__(self, args):
        if args not in self.memo:
            self.memo[args] = self.func(*args)
        return self.memo[args]


class Lookup(ABC):
    @abstractmethod
    def is_valid(self, code):
        pass

    @abstractmethod
    def get_code(self, value):
        pass

    @abstractmethod
    def get_value(self, lookup: tuple):
        return {}

    @classmethod
    def default(cls):
        return SqliteLookup()

    @classmethod
    def redis(cls):
        return RedisLookup()

    @classmethod
    def mysql(cls):
        return MySqlLookup()

    @classmethod
    def sqlite(cls):
        return SqliteLookup()


class RedisLookup(Lookup):
    def is_valid(self, code):
        """redis specific technical stuff"""
        return None  # replace with result of redis call

    def get_code(self, value):
        code = None  # dummy value for now

    def get_value(self, lookup):
        value = None  # dummy value for now
        return None


class MySqlLookup(Lookup):
    def __init__(self):
        pass

    def is_valid(self, code):
        """mysql specific technical stuff"""
        return True == True  # replace with result of mysql call

    def get_code(self, value):
        code = None  # dummy value for now

    def get_value(self, lookup: tuple):
        value = None  # dummy value for now
        return value


class SqliteLookup(Lookup):
    def __init__(self):
        self.con = sqlite3.connect("data/helo_ref.db")
        self.cur = self.con.cursor()

    def is_valid(self, code):
        return True == True

    def get_code(self, value):
        code = None

    def get_value(self, lookup: tuple):
        type, code = lookup
        result = self.cur.execute(
            f"SELECT value FROM helo_ref where type='{type}' and code='{code}';"
        )
        ref_data = result.fetchone()
        return {(type, code): ref_data}


if __name__ == "__main__":
    l = Lookup.default()
    value = l.get_value(("UNLOC", "USSFO"))
    print(value)
