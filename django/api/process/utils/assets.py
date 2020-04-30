import mantistable.settings
from api.process.utils.singleton import Singleton

import os
import json

@Singleton
class Assets():
    resource_dir = mantistable.settings.MANTIS_RES_DIR

    def get_asset(self, path):
        """
        Get asset raw content
        :param path:
        :return string:
        """
        p = os.path.join(self.resource_dir, path)
        with open(p, 'r') as asset:
            data = asset.read()

        return data

    def get_json_asset(self, path):
        """
        Get json asset
        :param path:
        :return python object:
        """
        return json.loads(self.get_asset(path))

    def get_list_asset(self, path):
        """
        Load an asset as 1D list
        :param path:
        :return:
        """
        return "".join(self.get_asset(path).split('\r')).split('\n')