import json
import os
import sys


dump = "linkages.json"

if len(sys.argv) != 3:
    print("""Usage:
python export_pred <linkages_json> <output_csv>""")
    sys.exit(-1)

dump = sys.argv[1]
out = sys.argv[2]


with open(dump) as f:
    with open(out, "w") as out:
        for line in f:
            content = json.loads(line)
            name = content["name"]
            linkages = content["linkages"]
            
            if len(linkages) == 0:
                continue
            
            row = linkages[0]
            if len(row) > 0:
                for col_idx, col in enumerate(row):
                    if col["predicate"] != None:
                        pred = col["predicate"]
                        out.write(f'"{name}","0","{col_idx+1}","{pred}"\n')
