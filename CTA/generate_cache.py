from lamapi_wrapper import LamAPIWrapper
from tqdm import tqdm
import json


def load_cea_ann():
    lines = open("CEA.csv").read().strip().split("\n")
    cea_ann = {}
    for line in lines:
        line = line[1:len(line)-1]
        cols = line.split('","')
        table_name = str(cols[0])
        col = str(cols[2])
        entity = str(cols[3])
        if table_name not in cea_ann:
            cea_ann[table_name] = {} 
        if col not in cea_ann[table_name]:
            cea_ann[table_name][col] = []   
        cea_ann[table_name][col].append(entity)
    return cea_ann      


def main():
    cea = load_cea_ann()
    lamapi = LamAPIWrapper("mantistable4_api_rest", 5000)
    cache = {}
    for table in tqdm(cea):
        for col in cea[table]:
            results = lamapi.concepts(cea[table][col])
            for entity in results:
                cache[entity] = results[entity]
         
    with open("cache.json", "w") as f:
        f.write(json.dumps(cache))            
    

if __name__ == "__main__":
    main()
