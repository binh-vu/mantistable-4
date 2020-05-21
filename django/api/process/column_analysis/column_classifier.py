from api.process.utils.nlp import utils as nlp

class ColumnClassifier:
    def __init__(self, cols):
        # TODO: from db cols["stats"]
        # 0: {NONE: 4, ID: 1, DATE: 5,...}
        self._cols = cols

    def get_columns_tags(self):
        tags = {}
        for col_name, col in self._cols.items():
            rows_count = sum([ freq for freq in col.values() ])
            lit_count = self._accumulate_freqs(
                col,
                lambda col_type: col_type != "STRING"
            )
            max_type = max(col, key=col.get)

            lit_type = None
            if lit_count > 0.60 * rows_count:
                lit_type = max_type

            if lit_type is not None:
                tags[col_name] = {
                    "tags": {
                        "col_type": "LIT",
                        "lit_type": lit_type
                    }
                }
            else:
                tags[col_name] = {
                    "tags": {
                        "col_type": "NE",
                        #"ne_type": None
                    }
                }

        return tags

    def _accumulate_freqs(self, col, pred):
        return sum([
            freq
            for col_type, freq in col.items()
            if pred(col_type)
        ])
