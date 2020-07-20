import re
import json
from .cleaner import Cleaner

import api.process.utils.nlp.utils as nlp

class CleanerLight(Cleaner):
    def _get_clean_text(self):
        clean_text = self._text

        clean_text = clean_text.strip().lower()
        clean_text = nlp.remove_extra_spaces(clean_text)
        return json.dumps(clean_text)[1:-1]