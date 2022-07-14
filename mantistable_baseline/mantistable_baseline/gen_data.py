"""Generate necessary files that mantistable needs for CTA and CPA tasks"""
import orjson, argparse
from typing import Mapping
from mantistable_baseline.config import ROOT_DIR
from kgdata.wikidata.models import WDClass
from kgdata.wikidata.db import get_wdclass_db
from sm.prelude import M
from pathlib import Path
from tqdm import tqdm


export_graph_di_file = ROOT_DIR / "CTA/export_graph_di.json"


def gen_files(wdclasses: Mapping[str, WDClass]):
    """Generate the following files:
    1. export_graph_di.json: following the format required by `networkx.readwrite.json_graph.node_link_graph`
    """
    global export_graph_di_file

    nodes = []
    links = []

    for wdclass in tqdm(wdclasses.values(), desc="Generate export_graph_di.json"):
        nodes.append({"id": wdclass.id})
        for parent in wdclass.parents:
            links.append({"source": parent, "target": wdclass.id})

    with open(export_graph_di_file, "wb") as f:
        f.write(
            orjson.dumps(
                {
                    "directed": True,
                    "multigraph": False,
                    "graph": {},
                    "nodes": nodes,
                    "links": links,
                }
            )
        )


def verify_export_graph_di_file(wdclasses: Mapping[str, WDClass]):
    global export_graph_di_file

    graph = orjson.loads(export_graph_di_file.read_text())
    missing = 0
    error = 0

    with tqdm(total=len(graph["links"])) as pbar:
        for link in graph["links"]:
            if link["source"] not in wdclasses or link["target"] not in wdclasses:
                missing += 1
            else:
                source = wdclasses[link["source"]]
                target = wdclasses[link["target"]]
                if source.id not in target.parents:
                    error += 1

            pbar.update(1)
            pbar.set_postfix(missing=missing, error=error)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a graph of parent-child classes in Wikidata"
    )
    parser.add_argument(
        "-d",
        "--db",
        default=ROOT_DIR / "../../data/home/databases",
        help="parent directory of wdclasses.db (generated using package kgdata)",
    )
    args = parser.parse_args()

    dbdir = Path(args.db)
    wdclasses = get_wdclass_db(
        dbdir / "wdclasses.db",
        read_only=True,
        proxy=False,
    )

    wdclasses = wdclasses.cache()
    for record in M.deserialize_jl(dbdir / "wdclasses.fixed.jl"):
        cls = WDClass.from_dict(record)
        wdclasses._cache[cls.id] = cls

    # verify_export_graph_di_file(wdclasses)
    gen_files(wdclasses)
