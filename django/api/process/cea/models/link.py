import json

class Link:
    def __init__(self, triple: tuple, confidence: float):
        assert(len(triple) == 3)
        # TODO: Meh, this is not true becouse I changed literal score for numeric types, but even if it works better than before this should be considered a bug
        # TODO: Check upper todo 'cos I think is old
        # assert(confidence >= 0.0 and confidence <= 1.0)

        self.triple = triple
        self.confidence = confidence

    def subject(self):
        return self.triple[0]

    def predicate(self):
        return self.triple[1]

    def object(self):
        return self.triple[2]

    def get_triple(self):
        return self.triple

    def get_confidence(self):
        return self.confidence

    def toJSON(self):   # TODO.... remove
        return json.dumps(
            self,
            default=lambda o: (self.triple, self.confidence), 
            sort_keys=True)

    def __str__(self):
        return f"{{{str(self.triple)}, {str(self.confidence)}}}"

    def __repr__(self):
        return self.__str__()