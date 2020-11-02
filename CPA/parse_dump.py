import json
import os
import sys


out = "linkages.json"

if len(sys.argv) != 4:
    print("""Usage:
python parse_dump.py <dump_name> <dataset_name> <output_json>""")
    sys.exit(-1)

dump = sys.argv[1]
dataset_name = sys.argv[2]
out = sys.argv[3]


with open(dump) as f:
    with open(out, "w") as out:
        for line in f:
            content = json.loads(line)
            name = content["name"]
            dataset = content["dataset_id"]
            if dataset == dataset_name:
                linkages = content["linkages"]
                
                o = {
                    "name": name[0:-5],
                    "linkages": linkages
                }
                json.dump(o, out)
                out.write("\n")
                
