import os

class Config:
    @staticmethod
    def get_env_variable(key):
        try:
            return os.environ[key]
        except KeyError:
            raise Exception(f"ERROR: Environment variable '{key}' not set.")
