from api.process.cea.models.cell import Cell

class Row:
    def __init__(self):
        self.subject_cell = None
        self.cells = []

    def add_ne_cell(self, content: str, normalized: str, candidates: list, is_subject=False):
        cell = Cell(content, normalized, candidates)
        self.cells.append(cell)

        if is_subject:
            self.subject_cell = cell

    def add_lit_cell(self, content: str, normalized: str, candidates: list):
        cell = Cell(content, normalized, candidates, True)
        self.cells.append(cell)

    def get_subject_cell(self):
        return self.subject_cell

    def get_cells(self):
        return self.cells

    def __len__(self):
        return len(self.cells)