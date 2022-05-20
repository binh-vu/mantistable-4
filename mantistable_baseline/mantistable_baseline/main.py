import sys
from pathlib import Path

from mantistable_baseline.config import ROOT_DIR

sys.path.append(str(ROOT_DIR / "django"))
sys.path.append(str(ROOT_DIR / "CTA"))

import sys
import uuid
from dataclasses import dataclass
from enum import Enum
from hashlib import md5
from operator import itemgetter
from typing import Dict, List, Literal, Mapping, Optional, Set, Tuple, TypedDict
from urllib.parse import urlparse

from api.my_tasks import (
    clean_up,
    computation_phase,
    data_preparation_phase,
    data_retrieval_phase,
)
from api.process.utils.lamapi.my_wrapper import LamAPIWrapper
from kgdata.wikidata.models import WDEntity
from sm.prelude import I, M
from tqdm.auto import tqdm

from mantistable_baseline.post_process import CPAMethod, Input, get_cpa, get_cta


Output = TypedDict(
    "MantisTableOutput",
    {
        "cpa": List[Tuple[int, int, str]],
        "cta": Dict[int, str],
        "method": Literal["mantis", "majority", "max-confidence"],
    },
)


Example = TypedDict(
    "Example",
    {
        "table": I.ColumnBasedTable,
        "links": Dict[Tuple[int, int], List[str]],
        "subj_col": Optional[Tuple[int, str]],
    },
)


def predict(
    qnodes: Mapping[str, WDEntity],
    examples: List[Example],
) -> List[List[Output]]:
    """Predict a list of output for each example."""
    inputs: List[Input] = []
    for example in examples:
        inputs.append(
            predict_step1(
                qnodes, example["table"], example["links"], example["subj_col"]
            )
        )

    cta_result = get_cta(inputs, qnodes)
    outputs = []

    for i, example in enumerate(examples):
        table_id = example["table"].table_id

        if table_id not in cta_result:
            print(f"Table {table_id} not found in CTA result")
            cta = {}
        else:
            cta = {
                int(cidx): class_id for cidx, class_id in cta_result[table_id].items()
            }

        output = []
        for cpa_method in CPAMethod:
            output.append(
                {
                    "cpa": get_cpa(
                        cpa_method,
                        col_tags=inputs[i].col_tags,
                        linkage=inputs[i].linkage,
                    ),
                    "cta": cta,
                    "method": cpa_method.value,
                }
            )
        outputs.append(output)

    return outputs


def predict_step1(
    qnodes: Mapping[str, WDEntity],
    tbl: I.ColumnBasedTable,
    links: Dict[Tuple[int, int], List[str]],
    subj_col: Optional[Tuple[int, str]],
) -> Input:

    # convert the table into mantis format
    mantis_table = []
    has_unique_label = len({c.name for c in tbl.columns}) == len(tbl.columns)
    if has_unique_label:
        name2index = {c.name: ci for ci, c in enumerate(tbl.columns)}
    else:
        name2index = {f"{c.name} {ci}": ci for ci, c in enumerate(tbl.columns)}
    if subj_col is not None:
        assert subj_col[1] == tbl.columns[subj_col[0]].name
        subj = subj_col[0]
    else:
        subj = None

    for ri in range(len(tbl.columns[0].values)):
        row = {}
        for ci, col in enumerate(tbl.columns):
            cname = col.name if has_unique_label else f"{col.name} {ci}"
            row[cname] = col.values[ri]
        mantis_table.append(row)

    # telling our lamapiwrapper we are working on this table
    LamAPIWrapper.set_table(tbl, name2index, links, qnodes)

    # submit the job
    job_id = str(uuid.uuid4())
    table_id, table_name = tbl.table_id, tbl.table_id
    workflow_tables = data_preparation_phase(
        [(table_id, table_name, mantis_table)], job_id
    )
    assert len(workflow_tables) == 1

    # force the result of subject column detection to be correct if subjs is supplied
    if subj is not None:
        print("Forcing subject columns to be:", subj_col)
        assert sum(
            int(cdata["tags"]["col_type"] == "SUBJ") == 1
            for cname, cdata in workflow_tables[0][2].items()
        ), "My understanding about only one subjects in the table of this method is incorrect"
        if any(
            cdata["tags"]["col_type"] == "SUBJ" and name2index[cname] != subj
            for cname, cdata in workflow_tables[0][2].items()
        ):
            # they incorrectly recognize the subject column, force the result to be correct
            for cname, cdata in workflow_tables[0][2].items():
                cindex = name2index[cname]
                if cindex == subj:
                    cdata["tags"]["col_type"] = "SUBJ"
                elif cdata["tags"]["col_type"] in {"SUBJ", "NE"}:
                    cdata["tags"]["col_type"] = "NE"

    data_retrieval_phase(workflow_tables, job_id)
    result = computation_phase(workflow_tables, job_id)
    assert len(result) == 1

    # get result
    linkage, column_tag, column_names = result[0]
    assert column_names == [
        name for name, index in sorted(name2index.items(), key=itemgetter(1))
    ]

    return Input(table=tbl, col_tags=column_tag, linkage=linkage)
