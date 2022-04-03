1) Put the complete CEA.csv in this folder
2) Run generate_cache.py to get all concepts from CEA entities
3) Run generate_candidates_cta.py to get candidate concepts
4) Run CTA.py to get cta.csv in output

---- Note:
They need a file called export_graph_di.json which containing the hierarchy of wikidata classes. 
This file is huge, so we ignore it. The original copy is found in the same folder but compressed.
To generate the file with current wikidata snapshot, use the code at: `mantistable_baseline/mantistable_baseline/gen_data.py`