from mantistable.celery import app
from api.models import Job
from api.serializers import TableSerializer
from api.process.utils import table
from api.process.utils.mongo.repository import Repository

from api.process.normalization import normalizer, cleaner
from api.process.column_analysis import column_classifier
from api.process.data_retrieval import cells as data_retrieval

from api.process.cea import cea
from api.process.cpa import cpa
from api.process.revision import revision

from celery import group
from multiprocessing import Manager

import requests
import json
import math

manager = Manager()
shared_memory = manager.dict()

def job_slot(job_id: int):
    job = Job.objects.get(id=job_id)
    tables = [
        (idx, table)
        for idx, table in enumerate(job.tables)
    ]

    workflow = data_preparation_phase.s(tables, job_id) | data_retrieval_phase.s() | computation_phase.s(job_id) | clean_up.si(job_id)
    return workflow.apply_async()

@app.task(name="data_preparation_phase", bind=True)
def data_preparation_phase(self, tables, job_id):
    self.replace(group([
        data_preparation_table_phase.s(job_id, *table)
        for table in tables
    ]))

@app.task(name="data_retrieval_phase", bind=True)
def data_retrieval_phase(self, tables):
    # TODO: Extract to utils
    def generate_chunks(iterable, n):
        assert (n > 0)
        for i in range(0, len(iterable), n):
            yield iterable[i:i + n]

    print(f"Data retrieval")

    cells_content = set()
    for table in tables:
        metadata = table[2]
        tags = [
            col_val["tags"]["col_type"]
            for col_val in metadata.values()
        ]
        cells = {
            values["normalized"]
            for col_idx, col_val in enumerate(metadata.values())
            for values in col_val["values"]
            if tags[col_idx] != "LIT"
        }
        cells_content.update(cells)

    cells_content = list(cells_content)
    # TODO: Extract constants
    THREADS = 4
    CHUNK_SIZE = int(math.ceil(len(cells_content) / THREADS))
    chunks = generate_chunks(cells_content, CHUNK_SIZE)

    self.replace(
        group([
            data_retrieval_group_phase.si(chunk)
            for chunk in chunks
        ]) | dummy_phase.si(tables)
    )

@app.task(name="computation_phase", bind=True)
def computation_phase(self, info, job_id):
    print("Computation")
    self.replace(
        group([
            computation_table_phase.s(job_id, *table)
            for table in info
        ])
    )


@app.task(name="data_preparation_table_phase")
def data_preparation_table_phase(job_id, table_id, table_data):
    job = Job.objects.get(id=job_id)

    print(f"Normalization")
    normalization_result = _normalization_phase(table_id, table_data)
    client_callback(job, table_id, "normalization", normalization_result)
    
    print(f"Column Analysis")
    col_analysis_result = _column_analysis_phase(table_id, table_data, normalization_result)
    client_callback(job, table_id, "column analysis", col_analysis_result)

    return table_id, table_data, col_analysis_result

@app.task(name="data_retrieval_group_phase")
def data_retrieval_group_phase(chunk):
    solr_result = data_retrieval.CandidatesRetrieval(chunk).get_candidates()

    print("Clean solr results")
    data_retrieval_result = {}
    for cell in solr_result.keys():
        for res in solr_result[cell]:
            label = res["label"]
            entity = res["uri"]

            norm_label = cleaner.Cleaner(label).clean()
            if cell not in data_retrieval_result:
                data_retrieval_result[cell] = []

            data_retrieval_result[cell].append((norm_label, entity))
    
    shared_memory.update(data_retrieval_result)

@app.task(name="dummy_phase")
def dummy_phase(info):
    return info

@app.task(name="computation_table_phase")
def computation_table_phase(job_id, table_id, table_data, columns):
    job = Job.objects.get(id=job_id)

    print("Computation table")
    candidates = shared_memory
    tags = [
        col_val["tags"]["col_type"]
        for col_val in columns.values()
    ]

    normalized = {
        values["original"]: values["normalized"]
        for col_val in columns.values()
        for values in col_val["values"]
    }

    print ("CEA")
    cea_results = cea.CEAProcess(
        table.Table(table_id=table_id, table=table_data),
        tags=tags,
        normalized_map=normalized,
        candidates_map=candidates
    ).compute()

    print(cea_results)

    print ("CPA")
    cpa_results = cpa.CPAProcess(
        cea_results
    ).compute()

    print(cpa_results)

    print ("Revision")
    revision_results = revision.RevisionProcess(
        cea_results, 
        cpa_results
    ).compute()
    client_callback(job, table_id, "computation", revision_results)

    print(revision_results)

    #return table_id, table_data, revision_results


@app.task(name="clean_up")
def clean_up(job_id):
    # TODO: For now just delete job
    Job.objects.get(id=job_id).delete()

def _normalization_phase(table_id, data):
    table_model = table.Table(table_id=table_id, table=data)
    metadata = normalizer.Normalizer(table_model).normalize()
    return metadata

def _column_analysis_phase(table_id, table, data):
    stats = {
        col_name: d["stats"]
        for col_name, d in data.items()
    }
    cc = column_classifier.ColumnClassifier(stats)
    tags = cc.get_columns_tags()
    
    metadata = {
        col_name: {**prev_meta, **tags[col_name]}
        for col_name, prev_meta in data.items()
    }

    return metadata

def _data_retrieval_phase(cells):
    solr_result = data_retrieval.CandidatesRetrieval(cells).get_candidates()

    print("Clean solr results")
    results = {}
    for cell in solr_result.keys():
        for res in solr_result[cell]:
            label = res["label"]
            entity = res["uri"]

            norm_label = cleaner.Cleaner(label).clean()
            if cell not in results:
                results[cell] = []

            results[cell].append((norm_label, entity))

    return results

# ====================================================

def client_callback(job, table_id, header: str, payload=None):
    assert header is not None and len(header) > 0   # Contract

    if payload is None:
        payload = { }

    message = {
        "job_id": job.id,
        "table_id": table_id,
        "header": header,
        "payload": payload
    }

    try:
        response = requests.post(job.callback, json=message)
        print("sent to:", job.callback)
    except Exception as e:
        # Send to morgue?
        print("Error", e)
        return

    if response.status_code != 200:
        # Send to morgue?
        print("Response error", response.status_code)
        pass