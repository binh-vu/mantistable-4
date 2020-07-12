from mantistable.celery import app
from api.models import Job
from api.process.utils import table
from api.process.utils.rules import person_rule as rules
from api.process.utils.mongo.repository import Repository

from api.process.normalization import normalizer, cleaner
from api.process.column_analysis import column_classifier
from api.process.data_retrieval import cells as cells_data_retrieval
from api.process.data_retrieval import links as links_data_retrieval

from api.process.cea import cea
from api.process.cea.models.cell import Cell
from api.process.cpa import cpa
from api.process.revision import revision

from celery import group
from multiprocessing import Manager

import requests
import json
import math

# TODO: EXtract constants
THREADS = 4
manager = Manager()
shared_memory = manager.dict()
literals_memory = manager.dict()

# TODO: Extract to utils
def generate_chunks(iterable, n):
    assert (n > 0)
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]


def job_slot(job_id: int):
    job = Job.objects.get(id=job_id)

    tables = [
        (item[0], item[1])
        for item in job.tables
    ]

    #data_preparation_phase(tables, job_id)
    workflow = data_preparation_phase.s(tables, job_id) | data_retrieval_phase.s(job_id) | computation_phase.s(job_id) | clean_up.si(job_id)
    return workflow.apply_async()

@app.task(name="data_preparation_phase", bind=True)
def data_preparation_phase(self, tables, job_id):
    self.replace(group([
        data_preparation_table_phase.s(job_id, *table)
        for table in tables
    ]))

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

@app.task(name="data_retrieval_phase", bind=True)
def data_retrieval_phase(self, tables, job_id):
    job = Job.objects.get(id=job_id)
    job.progress["current"] = 1
    job.save()


    print(f"Data retrieval")

    cells_content = set()
    for table in tables:
        metadata = table[2]
        tags = [
            col_val["tags"]["col_type"]
            for col_val in metadata.values()
        ]

        for col_idx, col_val in enumerate(metadata.values()):
            for values in col_val["values"]:
                if tags[col_idx] != "LIT":
                    # Apply rules
                    # TODO:
                    rule = rules.PersonRule(values["original"])
                    if rule.match():
                        query = rule.build_query()
                    else:
                        query = values["normalized"]

                    cells_content.add((query, values["original"]))
    
    cells_content = list(cells_content)

    CHUNK_SIZE = int(math.ceil(len(cells_content) / THREADS))
    chunks = generate_chunks(cells_content, CHUNK_SIZE)

    self.replace(
        group([
            data_retrieval_group_phase.si(job_id, chunk)
            for chunk in chunks
        ]) | data_retrieval_links_phase.si(job_id, tables)
    )

@app.task(name="data_retrieval_group_phase")
def data_retrieval_group_phase(job_id, chunk):
    job = Job.objects.get(id=job_id)
    elastic_result = cells_data_retrieval.CandidatesRetrieval(chunk, job.backend).get_candidates()

    print("Clean solr results")
    data_retrieval_result = {}
    for cell in elastic_result.keys():
        if len(elastic_result[cell]) == 0:
            client_callback(Job.objects.get(id=job_id), -1, "debug", f"No candidates for '{cell}'")

        for res in elastic_result[cell]:
            label = res["label"]
            entity = res["uri"]

            norm_label = cleaner.Cleaner(label).clean()
            if cell not in data_retrieval_result:
                data_retrieval_result[cell] = []
            
            data_retrieval_result[cell].append((norm_label, entity))
    
    shared_memory.update(data_retrieval_result)


@app.task(name="data_retrieval_links_phase", bind=True)
def data_retrieval_links_phase(self, job_id, tables):
    print(f"Data retrieval links")
    candidates = shared_memory

    def get_candidates(raw_cell, norm_cell):
        rule = rules.PersonRule(raw_cell)
        if rule.match():
            query = rule.build_query()
        else:
            query = norm_cell

        return candidates.get(query, [])

    pairs = []
    for table in tables:
        metadata = table[2]
        tags = [
            col_val["tags"]["col_type"]
            for col_val in metadata.values()
        ]

        rows = []
        pairs = {}
        for row_idx in range(0, len(metadata[list(metadata.keys())[0]]["values"])):
            rows.append([])
            for col_idx in range(0, len(metadata.keys())):
                values = metadata[list(metadata.keys())[col_idx]]["values"][row_idx]
                rows[-1].append(values)

        for values in rows:
            subject_cell_raw = values[0]["original"]
            subject_norm = values[0]["normalized"]
            subject_candidates = get_candidates(subject_cell_raw, subject_norm)
            object_cells = values[1:]

            for idx, obj_cell in enumerate(object_cells):
                obj_raw = obj_cell["original"]
                
                if tags[idx + 1] != "LIT":
                    obj_norm = obj_cell["normalized"]
                else:
                    obj_norm = obj_raw

                obj_candidates = get_candidates(obj_raw, obj_norm)

                pair = (
                    (subject_cell_raw, subject_norm, subject_candidates, False),
                    (obj_raw, obj_norm, obj_candidates, tags[idx + 1] == "LIT"),
                )
                pairs[(subject_cell_raw, obj_raw)] = pair

    CHUNK_SIZE = 20
    chunks = generate_chunks(list(pairs.values()), CHUNK_SIZE)

    self.replace(
        group([
            data_retrieval_links_group_phase.si(job_id, chunk)
            for chunk in chunks
        ]) | dummy_phase.s(tables)
    )

@app.task(name="data_retrieval_links_group_phase")
def data_retrieval_links_group_phase(job_id, chunk):
    job = Job.objects.get(id=job_id)
    links = links_data_retrieval.LinksRetrieval(chunk, job.backend).get_links()

    links = {
        hash(k): v
        for k,v in links.items()
    }

    return links

@app.task(name="dummy_phase")
def dummy_phase(triples, tables):
    joined_triples = {}
    for triple in triples:
        joined_triples.update(triple)

    return tables, joined_triples

@app.task(name="computation_phase", bind=True)
def computation_phase(self, info, job_id):
    tables, triples = info
    job = Job.objects.get(id=job_id)
    job.progress["current"] = 2
    job.save()

    print("Computation")
    print(triples, tables)
    self.replace(
        group([
            computation_table_phase.s(job_id, triples, *table)
            for table in tables
        ])
    )

@app.task(name="computation_table_phase")
def computation_table_phase(job_id, triples, table_id, table_data, columns):
    job = Job.objects.get(id=job_id)

    print("Computation table")
    tags = [
        col_val["tags"]["col_type"]
        for col_val in columns.values()
    ]

    normalized = {
        values["original"]: values["normalized"]
        for col_val in columns.values()
        for values in col_val["values"]
    }

    candidates = {
        norm: shared_memory.get(norm, {})
        for norm in normalized.values()
    }

    print ("CEA")
    cea_results = cea.CEAProcess(
        table.Table(table_id=table_id, table=table_data),
        triples=triples,
        tags=tags,
        normalized_map=normalized,
        candidates_map=candidates
    ).compute(job.backend)

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

    #print(revision_results)

    #return table_id, table_data, revision_results


@app.task(name="clean_up")
def clean_up(job_id):
    job = Job.objects.get(id=job_id)
    job.progress["current"] = 3
    job.save()

    client_callback(job, -1, "end", {})

    # TODO: For now just delete job
    job.delete()


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