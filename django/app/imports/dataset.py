from app.models import Table, Dataset
import json
import csv
import zipfile

class DatasetImport:
    def __init__(self, dataset_name, dataset_file):
        self._dataset_name = dataset_name
        self._dataset_file = dataset_file
        self._tables = []
        self._rows = 0
        self._cols = 0

    def load(self):
        dataset = self._get_dataset()
        
        tables = []
        ext = str(self._dataset_file).split(".")[-1]
        if ext == "zip":
            unzipped = zipfile.ZipFile(self._dataset_file)
            tables = self._load_zip(unzipped)
        elif ext == "csv":
            tables = self._load_csv(self._dataset_file.name, self._dataset_file.read().decode('utf-8'))
        else:
            tables = self._load_json(self._dataset_file.name, self._dataset_file.read().decode('utf-8'))

        rows_count = 0
        cols_count = 0
        for table in tables:
            table_name, table_data = table
            
            rows, cols = self._table_stats(table_data)
            rows_count += rows
            cols_count += cols
            
            self._save_table(dataset, table_name, table_data, rows, cols)

        dataset.table_count = dataset.table_set.count()
        tableofdataset = dataset.table_set.all()
        dataset.average_rows = sum([t.rows_count for t in tableofdataset]) / dataset.table_count
        dataset.average_cols = sum([t.cols_count for t in tableofdataset]) / dataset.table_count

        dataset.save()

    def _load_csv(self, table_name, content):
        f = []
        for row in csv.DictReader(content):
            f.append(row)

        return [(table_name, f)]

    def _load_json(self, table_name, content):
        return [(table_name, json.loads(content))]

    def _load_zip(self, zipfile):
        content = []
        for libitem in zipfile.namelist():
            data = zipfile.read(libitem).decode('utf-8')

            if libitem.startswith('__'):
                continue

            if libitem.endswith('csv'):
                content.extend(self._load_csv(libitem, data))
            elif libitem.endswith('json'):
                content.extend(self._load_json(libitem, data))
            else:
                continue

        return content

    def _get_dataset(self):
        dataset = Dataset.objects.filter(name=self._dataset_name)
        if not dataset.exists():
            dataset = Dataset(
                name=self._dataset_name,
                table_count=0,
                average_rows=0,
                average_cols=0
            )
            dataset.save()
        else:
            dataset = dataset[0]

        return dataset

    def _table_stats(self, content):
        rows = len(content)
        cols = len(content[0].keys()) if rows > 0 else 0
        return rows, cols

    def _save_table(self, dataset, name, content, rows, cols):
        Table(
            name=name,
            dataset=dataset,
            original=content,
            rows_count=rows,
            cols_count=cols
        ).save()