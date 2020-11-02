from tqdm import tqdm
import json

def get_concetps(cache, entities):
    new_entities = {}
    for entity in entities:
        if entity in cache:
            new_entities[entity] = cache[entity]
    return getConceptsFreq(new_entities)

def getConceptsFreq(entities):
    temp = {}
    for entity in entities:
        for concept in entities[entity]:
            if concept not in temp:
                temp[concept] = 0
            temp[concept] += 1
    max = get_max(temp)       
    for key in temp:
        temp[key] /= max
    return temp

def get_max(temp):
    if len(temp) == 0:
        return -1
    max = 0
    for key in temp:
        if temp[key] > max:
            max = temp[key]
    return max        

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
    cta = {}
    with open("cache.json") as f:
        cache = json.loads(f.read())
    for table in tqdm(cea):
        for col in cea[table]:
            global_concepts = get_concetps(cache, cea[table][col])
            if len(global_concepts) == 0:
                continue
            if table not in cta:
                cta[table] = {}
            cta[table][col] = global_concepts
   
    with open("export_cta.json", "w") as f:
        f.write(json.dumps(cta, indent=4))



if __name__ == "__main__":
    main()