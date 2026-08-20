import json

class ApiConn:
    def __init__(self, config_path="api_conn_config.json"):
        with open(config_path) as f:
            config = json.load(f)
        self.password = config["password"]
        self.host = config["host"]
        self.login = config["login"]
        self.extra_dejson = config["extra"]

api_conn = ApiConn()