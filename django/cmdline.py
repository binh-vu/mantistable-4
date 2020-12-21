from api.my_tasks import data_preparation_phase, data_retrieval_phase, computation_phase, clean_up

import json
import os

if __name__ == "__main__":
  location = './test_data'
  files_in_dir = []
  for r, d, f in os.walk(location):
    for item in f:
        if '.json' in item:
          files_in_dir.append(os.path.join(r, item))

  print(files_in_dir)
  
  tables = []
  for idx, f in enumerate(files_in_dir):
    with open(f, 'r') as myfile:
      data = json.load(myfile)
      tables.append((idx, os.path.basename(f), data))

  job_id = 1
  
  # make the workflow synchronous
  workflow_tables = data_preparation_phase(tables, job_id)
  data_retrieval_phase(workflow_tables, job_id)
  workflow_tables = computation_phase(workflow_tables, job_id)

  print("done")

# tables payload example
# [
#   [
#     6, 
#     'test', 
#     [
#       {'MOUNTAIN': 'Mount Everest', 'HEIGHT IN METERS': '8,848', 'RANGE': 'Himalayas', 'CONQUERED ON': 'May 29, 1953', 'COORDINATES': '27.98785, 86.92502609999997', 'URL': 'https://en.wikipedia.org/wiki/Mount_Everest'}, {'MOUNTAIN': 'K-2 (Godwin Austin)', 'HEIGHT IN METERS': '8,611', 'RANGE': 'Karakoram', 'CONQUERED ON': 'July 31, 1954', 'COORDINATES': '35.8799875,76.51510009999993', 'URL': 'https://en.wikipedia.org/wiki/K2'}, {'MOUNTAIN': 'Kanchenjunga', 'HEIGHT IN METERS': '8,597', 'RANGE': 'Himalayas', 'CONQUERED ON': 'May 25, 1955', 'COORDINATES': '27.7024914,88.14753500000006', 'URL': 'https://en.wikipedia.org/wiki/Kangchenjunga'}
#     ]
#   ]
# ]
