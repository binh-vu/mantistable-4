class Table:
    def __init__(self, table_id, table: list):
        assert(isinstance(table, list))
        assert(len(table) > 0)

        self._table_id = table_id
        self._table = table

    def get_headers(self):
        return list(self._table[0].keys())

    def get_cols(self):
        headers = self.get_headers()
        cols = {}
        for header in headers:
            cols[header] = []
            for row in self._table:
                cols[header].append(row[header])

        return cols

    def get_rows(self):
        return self._table


    # TODO: I hate this
    @property
    def table_id(self):
        return self._table_id