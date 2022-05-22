# Overview

Code to run mantistable 4. This is not intend to be installed as a package, but to run directly from the folder as it imports original code relatively from this location.

List of local packages that this depends on:

- `django.api.my_tasks`
- `django.api.process.utils.lamapi.my_wrapper`

It also needs the file: `CTA/export_graph_di.json`, you can generate the file by running `python -m mantistable_baseline.gen_data`.
