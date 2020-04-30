from api.process.utils.singleton import Singleton
import mantistable.settings

from pymongo import MongoClient
from pymongo import UpdateOne

@Singleton
class MongoWrapper():
    def __init__(self):
        host = mantistable.settings.DATABASES["default"]["HOST"]
        port = mantistable.settings.DATABASES["default"]["PORT"]
        dbname = mantistable.settings.DATABASES["default"]["NAME"]
        
        self._client = MongoClient(f'mongodb://{host}:{port}/')
        self._db = self._client[dbname]

    def update_many(self, collection_name, docs: list):
        with self._client:
            self._db[collection_name].bulk_write([
                UpdateOne({'id': doc["id"]}, {'$set': doc})
                for doc in docs
                if "id" in doc
            ])


@Singleton
class Repository():
    def __init__(self):
        self._mongo = MongoWrapper()

    def write_tables(self, tables):
        # TODO: Write the entire tables in bulk
        raise NotImplementedError()

    # TODO: Deprecate these in favour of write_tables
    def write_cols(self, data):
        tables = [
            {
                "id": doc_id,
                "cols": col
            }
            for doc_id, col in data
        ]
        self._mongo.update_many("app_table", tables)

    def write_predicates(self, predicates):
        raise NotImplementedError()

    def write_concepts(self, concepts):
        raise NotImplementedError()