import requests
import json

from api.process.utils.decorators import retry_on_exception

class LamAPIWrapper:
    ACCESS_TOKEN = "ee4ba0c4f8db0eb3580cb3b7b5536c54"

    def __init__(self, endpoint, port=8093):
        self._endpoint = endpoint
        self._port = port

    @retry_on_exception(max_retries=5)
    def labels(self, query):
        response = requests.get(self._api_url("labels"), params={
            "query": query,
            "token": self.ACCESS_TOKEN
        })

        if response.status_code != 200:
            return None

        return response.json()["results"]

    @retry_on_exception(max_retries=5)
    def predicates(self, data):
        response = requests.post(self._api_url("predicates"),
            json={"json": data},
            params={"token": self.ACCESS_TOKEN}
        )

        if response.status_code != 200:
            return None

        return response.json()

    @retry_on_exception(max_retries=5)
    def objects(self, subjects):
        response = requests.post(self._api_url("objects"),
            json={"json": subjects},
            params={"token": self.ACCESS_TOKEN}
        )

        if response.status_code != 200:
            return None

        return response.json()

    @retry_on_exception(max_retries=5)
    def concepts(self, entities):
        response = requests.post(self._api_url("concepts"),
            json={"json": entities},
            params={"token": self.ACCESS_TOKEN}
        )

        if response.status_code != 200:
            return None

        return response.json()

    def _api_url(self, suburl):
        return f"http://{self._endpoint}:{self._port}/{suburl}"

if __name__ == "__main__":
    lapiw = LamAPIWrapper("149.132.176.50", 8093)
    res = lapiw.labels("batman")
    print(json.dumps(res, indent=4))
    print(len(res))