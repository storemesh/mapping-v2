import redis
import json
import pickle
import numpy as np
import re
from rank_bm25 import BM25Okapi
from tqdm.auto import tqdm
from sklearn.feature_extraction.text import TfidfVectorizer

from .tokenizer import TrigramsTokenizer, RobertaTokenize

class MDMorganiztion:
    def __init__(self, redis_url="redis://localhost:6379", verbose=False):
        self.verpose = verbose
        self.redis_con = redis.from_url(f"{redis_url}/10")
        if self.redis_con.ping() == False:
            raise Exception(f"Can not connect to redis {redis_url}")

        tokenizer = TrigramsTokenizer().tokenize
        self.vectorizer = TfidfVectorizer(tokenizer=tokenizer)

    def index(self, master):
        self.redis_con.set('master-org', json.dumps(master))
        documents = [v.get('name') for k,v in master.items()]
        tfidf_matrix = self.vectorizer.fit_transform(documents)
        self.redis_con.set('mdm-org-tfidf_matrix', pickle.dumps(tfidf_matrix))
        self.redis_con.set('mdm-org-vectorizer', pickle.dumps(self.vectorizer))
        

    def load(self):
        master = json.loads(self.redis_con.get('master-org'))
        self.master = {int(k): v for k,v in master.items()}

        self.tfidf_matrix = pickle.loads(self.redis_con.get('mdm-org-tfidf_matrix'))
        self.vectorizer = pickle.loads(self.redis_con.get('mdm-org-vectorizer'))

    def _rank(self, query):
        query_tfidf = self.vectorizer.transform([query])
        scores = np.dot(query_tfidf, self.tfidf_matrix.T).toarray()[0]
        return scores

    def mdm(self, query):
        query = re.sub(" \(มหาชน\)| จำกัด|บมจ.|บจ.|บริษัท |หจ.", "", query)
        if self.redis_con.exists(query):
            if self.verbose: print("use cached")
            return json.loads(self.redis_con.get(query))
        scores = self._rank(query=query)
        pred = np.argmax(scores)
        data = self.master[pred]
        data.update({'score': scores[pred]})
        self.redis_con.set(query, json.dumps(data))
        if self.verbose: print("new compute")
        return data
    
    def mapping(self, query, thresh=0.6):
        data = self.mdm(query=query)
        if data.get('score') >= thresh:
            return data.get('id')
        return ""
        
class MDMperson:
    def __init__(self, redis_url="redis://localhost:6379", verbose=False):
        self.verbose = verbose
        self.redis_con = redis.from_url(f"{redis_url}/11")
        if self.redis_con.ping() == False:
            raise Exception(f"Can not connect to redis {redis_url}")

        self.tokenizer = RobertaTokenize()

    def index(self, master):
        documents = [v.get('name') for k,v in master.items()]
        tokenized_documents = [self.tokenizer.tokenize(name) for name in tqdm(documents)]

        bm25 = BM25Okapi(tokenized_documents)

        self.redis_con.set('master-person', json.dumps(master))
        self.redis_con.set('mdm-person-bm25', pickle.dumps(bm25))
        

    def load(self):
        master = json.loads(self.redis_con.get('master-person'))
        self.master = {int(k): v for k,v in master.items()}

        self.bm25 = pickle.loads(self.redis_con.get('mdm-person-bm25'))

    def mdm(self, query):
        if self.redis_con.exists(query):
            if self.verbose: print("use cached")
            return json.loads(self.redis_con.get(query))
        tokenized_query = self.tokenizer.tokenize(query)
        bm25_scores = self.bm25.get_scores(tokenized_query)
        pred = np.argmax(bm25_scores)
        data = self.master[pred]
        data.update({'score': bm25_scores[pred]})
        self.redis_con.set(query, json.dumps(data))
        if self.verbose: print("new compute")
        return data
    
    def mapping(self, query, thresh=20):
        data = self.mdm(query=query)
        if data.get('score') >= thresh:
            return data.get('mdm')
        return ""