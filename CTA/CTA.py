import json
import copy
import networkx as nx
import os
from networkx.readwrite import json_graph
from tqdm import tqdm

def read_json_file(filename):
    with open(filename) as f:
        js_graph = json.load(f)
    return json_graph.node_link_graph(js_graph)


def get_predecessors(nodes):
    predecessors = []
    for node in nodes:
        for predecessor in graph.predecessors(node):
            predecessors.append(predecessor)
    return list(set(predecessors))    
    

def has_common_ancestor(concepts):
    ancestors = {}
    for concept in concepts:
        ancestors[concept] = set(graph.predecessors(concept))
    for u in ancestors:
        for v in ancestors:
            if u == v:
                continue
            intersection = ancestors[u].intersection(ancestors[v])
            if len(intersection) > 0:
                return True
    return False                


def compute_links(concepts):
    stats = {concept:0 for concept in concepts}
    for u in concepts:
        for v in concepts:    
            if u == v:
                continue
            if graph.has_successor(u, v):
                stats[u] += 1
    return stats            

def compute_common_ancestors_gen(concepts):
    ancestors_stats = {concept:0 for concept in concepts}
    for c1 in concepts:
        ancestors_c1 = set(graph.predecessors(c1))
        for c2 in concepts:
            if c1 == c2:
                continue      
            ancestors_c2 = set(graph.predecessors(c2))
            result = list(ancestors_c1.intersection(ancestors_c2))     
            for item in result:
                if item not in ancestors_stats:
                    ancestors_stats[item] = 0
                ancestors_stats[item] += 1    
    return ancestors_stats    

def get_common_ancestors(c1, c2):
    ancestors_c1 = set(graph.predecessors(c1))
    ancestors_c2 = set(graph.predecessors(c2))
    return list(ancestors_c1.intersection(ancestors_c2))


def get_common_ancestors_gen(concepts):
    assert len(concepts) > 0
    ancestors_stats = {}
    for c1 in concepts:
        ancestors_c1 = set(graph.predecessors(c1))
        for c2 in concepts:
            if c1 == c2:
                continue      
            ancestors_c2 = set(graph.predecessors(c2))
            result = list(ancestors_c1.intersection(ancestors_c2))     
            if len(result) == 0:  
                if c1 in ancestors_c2:
                    result.append(c1)
                elif c2 in ancestors_c1:
                    result.append(c2)
            for item in result:
                if item not in ancestors_stats:
                    ancestors_stats[item] = 0
                ancestors_stats[item] += 1    
    return ancestors_stats    

def get_ancestors(concepts, level):
    new_concepts = copy.deepcopy(concepts)
    predecessors = {}
    predecessors[0] = concepts
    for i in range(0, level):    
        predecessors[i+1] = get_predecessors(predecessors[i])
        new_concepts.extend(predecessors[i+1])    
    return list(set(new_concepts))

def compute_predecessors_links(concepts):
    assert len(concepts) > 0
    ancestors_stats = {}
    for c1 in concepts:
        ancestors_c1 = set(graph.predecessors(c1))
        for c2 in concepts:
            if c1 == c2:
                continue    
            ancestors_c2 = set(graph.predecessors(c2))
            
            for item in result:
                if item not in ancestors_stats:
                    ancestors_stats[item] = 0
                ancestors_stats[item] += 1    
    return ancestors_stats    

def get_final_concepts(concepts):
    new_concepts = copy.deepcopy(concepts)
    if len(concepts) == 1:
        return concepts
    if len(concepts) == 2:
        if graph.has_successor(concepts[1], concepts[0]):
            return [concepts[0]] 
        if graph.has_successor(concepts[0], concepts[1]):
            return [concepts[1]]
    links_score = compute_links(concepts)
    final_score = {item: links_score[item] for item in links_score}
    min_val = min(final_score.values())
    result = [item for item in final_score if final_score[item] == min_val]   
    return result




graph = read_json_file("export_graph_di.json")

with open("concepts.json") as f:
    concepts_list = json.loads(f.read())

input = open("candidates_cta.json", "r")
cta = json.loads(input.read())
input.close()
annotations = {}

with open("cta.csv", "w") as f:
    for id_table in tqdm(cta):
        annotations[id_table] = {}    
        for id_col in cta[id_table]:
            concepts = []    
            for concept in cta[id_table][id_col]:
                if cta[id_table][id_col][concept] >= 0.90:
                    if concept not in concepts_list:
                        graph.add_node(concept)
                    concepts.append(concept)

            if len(concepts) == 0: #skip write line on file
                continue

            final_concepts = get_final_concepts(concepts)

            f.write(f'"{id_table}","{int(id_col)}","{" ".join(final_concepts)}"\n')