from transformers import XLMRobertaTokenizer

class TrigramsTokenizer:
    def __init__(self):
        self.n = 3

    def tokenize(self, text):
        return [text[i:i+self.n] for i in range(len(text)-self.n+1)]
    
class RobertaTokenize:
    def __init__(self):
        self.tokenizer = XLMRobertaTokenizer.from_pretrained('xlm-roberta-base')

    def tokenize(self, query):
        return self.tokenizer.tokenize(query)