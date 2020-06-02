from api.process.normalization.cleaner import Cleaner
from api.process.utils.datatype import DataTypeEnum
from api.process.utils.table import Table
from api.process.utils.validator import Validator


import datatype
import dateutil.parser as dateutil

        
class Normalizer:
    def __init__(self, table: Table):
        self._table = table

    def normalize(self):
        metadata = {}
        for header, cells in self._table.get_cols().items():
            stats = {}
            metadata[header] = {
                'stats': {},
                'values': []
            }
            for value in cells:
                value_datatype = self._get_datatype(value)
                if value_datatype.get_xsd_type().label() == "xsd:string":
                    clean_text = self._get_clean_text(value)
                    value_datatype = self._get_datatype(clean_text)

                clean_text = self._get_uniform_datatype(value_datatype)

                if value_datatype.get_type().name not in stats:
                    stats[value_datatype.get_type().name] = 0
                stats[value_datatype.get_type().name] += 1

                metadata[header]['values'].append({
                    "datatype": value_datatype.get_type().name,
                    "original": value,
                    "normalized": clean_text
                })

            metadata[header]['stats'] = stats

        return metadata

    def _get_clean_text(self, value):
        return Cleaner(value).clean()

    def _get_uniform_datatype(self, value_datatype):
        return str(value_datatype.to_python())

    def _get_datatype(self, value):
        return datatype.get_datatype(value)