from api.process.normalization.tokenizer import Tokenizer, TokenTagEnum

from nltk.corpus import stopwords

class Cleaner:
    def __init__(self, text):
        self._text = text
        self._tokenizer = Tokenizer()

    def clean(self):
        clean_text = self._get_clean_text()
        return clean_text
        ''' TODO
        tokens = [
            word.strip()
            for word in clean_text.split(" ")
        ]
        relevant_tokens = filter(lambda item: self._is_relevant_word(item), tokens)

        return " ".join(list(relevant_tokens))
        '''

    def _get_clean_text(self):
        clean_text = ""
        tokens = self._tokenizer.tokenize(self._text)

        i = 0
        token = self._get_token(tokens, i)
    
        while token is not None:
            value = token.value
            tag = token.tag

            if tag == TokenTagEnum.WORD or tag == TokenTagEnum.NUMBER:
                if "'" in value:
                    index = value.find("'")
                    value = value[0:index]
                        
                if value != "":
                    clean_text += value + " "
            elif tag == TokenTagEnum.URL:
                clean_text += value
            elif value == "(":  # NOTE: skip parenthesis (...)
                j = i + 1
                forward_token = self._get_token(tokens, j)
                while forward_token is not None and forward_token.value != ")":
                    j += 1
                    forward_token = self._get_token(tokens, j)

                i = j
            elif value == "[":  # NOTE: skip parenthesis [...]
                j = i + 1
                forward_token = self._get_token(tokens, j)
                while forward_token is not None and forward_token.value != "]":
                    j += 1
                    forward_token = self._get_token(tokens, j)

                i = j
            elif value == "'":
                if len(clean_text) > 0 and clean_text[-1] == " ":
                    clean_text = clean_text[0:-1]
                    clean_text += value

            i += 1
            token = self._get_token(tokens, i)

        return clean_text.strip().lower()

    def _get_token(self, tokens, idx):
        if len(tokens) <= idx:
            return None

        return tokens[idx]

    def _is_relevant_word(self, word):
        return word not in stopwords.words('english')   # TODO: What about non-english stopwords??
