from django.db import models
from djongo.models.json import JSONField
import json

# NOTE: MongoDB does not allow '.' character in the document's keys (to avoid nasty NoSQL injection attacks). PyMongo
# enforce this rule by checking keys prior to insertion. To circumvent this limitation/security I propose a classic
# encode/decode solution. Prior to insertion dot character '.' is substituted with "\u002e" (the string,
# not the character). Any time the field is requested the string "\u002e" is substituted with the '.' character.
# NOTE: It should work seamlessly (you can use the '.' character) with any query operation like Model.objects.filter(
# ), but I haven't tested it yet...
class SafeJSONField(JSONField):
    translation_map = {
        "\\": "\\u005C",
        ".": "\\u002e",
        "$": "\\u0024",
    }
    reversed_translation_map = {v: k for k, v in translation_map.items()}

    def from_db_value(self, value, expr, conn):
        if value is None:
            return value

        return self.__decode(value)

    def to_python(self, value):
        if value is None:
            return value

        return self.__decode(value)

    def get_prep_value(self, value):
        return self.__encode(value)

    def __update_keys(self, json_data, func):
        if isinstance(json_data, list):
            array = []
            for item in json_data:
                array.append(self.__update_keys(item, func))

            return array

        if isinstance(json_data, dict):
            d = {}
            for k in json_data.keys():
                d[func(k)] = self.__update_keys(json_data[k], func)

            return d

        return json_data

    def __encode(self, json_data):
        return self.__update_keys(json_data, lambda k: self.__translate(k, SafeJSONField.translation_map))

    def __decode(self, json_data):
        return self.__update_keys(json_data, lambda k: self.__translate(k, SafeJSONField.reversed_translation_map))

    def __translate(self, key, translation_map):
        for mongo_chr in translation_map:
            key = str(key).replace(mongo_chr, translation_map[mongo_chr])

        return key

    # Json pretty print
    def __str__(self):
        return json.dumps(super.__str__(), indent=4)

class Dataset(models.Model):
    name = models.CharField(max_length=255, primary_key=True)
    table_count = models.PositiveIntegerField()
    average_rows = models.FloatField()
    average_cols = models.FloatField()

class Table(models.Model):
    name = models.CharField(max_length=255)
    dataset = models.ForeignKey(to=Dataset, on_delete=models.CASCADE)   # TODO: check this
    original = SafeJSONField(default=[])
    cols = SafeJSONField(default=[])
    predicates = SafeJSONField(default=[])
    concepts = SafeJSONField(default=[])

    rows_count = models.PositiveIntegerField()
    cols_count = models.PositiveIntegerField()
