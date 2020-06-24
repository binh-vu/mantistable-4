import yaml

class Config:
    def __init__(self, config_path):
        self._config_path = config_path
        self._data = self._parse()

    def _parse(self):
        content = {}
        with open(self._config_path, "r") as config_file:
            try:
                content = yaml.safe_load(config_file)
            except Exception as e:
                raise e         # Explicitly raise exception

        return content

    def __getitem__(self, key):
        return self._data[key]

    def __repr__(self):
        return repr(self._data)

    def __len__(self):
        return len(self._data)