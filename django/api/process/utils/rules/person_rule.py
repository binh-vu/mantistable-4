import re

class PersonRule:
    def __init__(self, cell_content):
        self.cell_content = cell_content
        
    def match(self):
         return re.search(r"^([\w&]\.\s*)+(\w+\'*)+", self.cell_content, re.IGNORECASE | re.UNICODE)
        
    def build_query(self):
        fields = list(map(lambda field: field.strip(), self.cell_content.split(".")))
        for idx, field in enumerate(fields):
            if len(field) == 1:
                fields[idx] = fields[idx] + "*"
            else:
                break
        
        return " ".join(fields).lower()
        
    def build_label(self, label):
        fields = label.split(" ")
        if len(fields[0]) > 0:
            fields[0] = fields[0][0]

        return " ".join(fields)