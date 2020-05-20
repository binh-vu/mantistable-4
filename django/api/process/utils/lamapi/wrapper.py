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
        response = self._make_request(
            lambda: requests.get(
                self._api_url("labels"),
                params={"query": query, "token": self.ACCESS_TOKEN}, 
                timeout=30
            )
        )

        if "results" in response:
            return response["results"]

        return response

    @retry_on_exception(max_retries=5)
    def predicates(self, data):
        return self._make_request(
            lambda: requests.post(
                self._api_url("predicates"),
                json={"json": data},
                params={"token": self.ACCESS_TOKEN},
                timeout=30
            )
        )

    @retry_on_exception(max_retries=5)
    def objects(self, subjects):
        return self._make_request(
            lambda: requests.post(
                self._api_url("objects"),
                json={"json": subjects},
                params={"token": self.ACCESS_TOKEN},
                timeout=30
            )
        )

    @retry_on_exception(max_retries=5)
    def literals(self, subjects):
        return self._make_request(
            lambda: requests.post(
                self._api_url("literals"),
                json={"json": subjects},
                params={"token": self.ACCESS_TOKEN},
                timeout=30
            )
        )

    @retry_on_exception(max_retries=5)
    def concepts(self, entities):
        return self._make_request(
            lambda: requests.post(
                self._api_url("concepts"),
                json={"json": entities},
                params={"token": self.ACCESS_TOKEN},
                timeout=30 
            )
        )

    def _api_url(self, suburl):
        return f"http://{self._endpoint}:{self._port}/{suburl}"

    def _make_request(self, requester):
        response = requester()
        if response.status_code != 200:
            return {}

        return response.json()


"""
if __name__ == "__main__":
    lapiw = LamAPIWrapper("149.132.176.50", 8093)
    res = lapiw.labels("batman")
    print(json.dumps(res, indent=4))
    print(len(res))
"""