from api.process.normalization.cleaner import Cleaner
from api.process.utils.datatype import DataTypeEnum
from api.process.utils.table import Table
from api.process.utils.validator import Validator

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
                datatype   = self._get_datatype(value)
                clean_text = self._get_clean_text(value)
                clean_text = self._get_uniform_datatype(datatype, clean_text)

                if datatype.name not in stats:
                    stats[datatype.name] = 0
                stats[datatype.name] += 1

                metadata[header]['values'].append({
                    "datatype": datatype.name,
                    "norm_cell": clean_text
                })

            metadata[header]['stats'] = stats

        return metadata

    def _get_clean_text(self, value):
        return Cleaner(value).clean()

    def _get_uniform_datatype(self, datatype, value):
        if datatype == DataTypeEnum.NUMERIC:
            return value.replace(",", "")
        elif datatype == DataTypeEnum.DATE:
            try:
                return dateutil.parse(value).strftime("%Y-%d-%m")
            except:
                return value

        return value

    def _get_datatype(self, value):
        validator = Validator()
        type_map = [
            # type, predicate
            (DataTypeEnum.EMPTY, validator.is_empty),
            (DataTypeEnum.NUMERIC, validator.is_numeric),
            (DataTypeEnum.BOOLEAN, validator.is_boolean),
            (DataTypeEnum.DATE, validator.is_date),
            (DataTypeEnum.GEOCOORD, validator.is_geocoords),
            (DataTypeEnum.DESCRIPTION, validator.is_description),
            (DataTypeEnum.NOANNOTATION, validator.is_no_annotation),
            (DataTypeEnum.ID, validator.is_id),
            (DataTypeEnum.URL, validator.is_url),
            (DataTypeEnum.EMAIL, validator.is_email),
            (DataTypeEnum.ADDRESS, validator.is_address),
            (DataTypeEnum.HEXCOLOR, validator.is_hexcolor),
            (DataTypeEnum.IP, validator.is_ip),
            (DataTypeEnum.CREDITCARD, validator.is_creditcard),
            (DataTypeEnum.IMAGE, validator.is_image),
            (DataTypeEnum.ISBN, validator.is_isbn),
            (DataTypeEnum.ISO8601, validator.is_iso8601),
            (DataTypeEnum.CURRENCY, validator.is_currency),
            (DataTypeEnum.IATA, validator.is_iata),
        ]

        for (data_type, predicate) in type_map:
            if predicate(value):
                return data_type

        return DataTypeEnum.NONE