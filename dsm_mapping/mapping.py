import requests
from . import utils
from .utils.resultify import resultify
from tqdm.auto import tqdm
import dask.dataframe as dd
import pandas as pd
from dask.diagnostics import ProgressBar

import dask
from multiprocessing.pool import ThreadPool

def chunker(seq, size):
    return ((pos, seq[pos:pos + size]) for pos in range(0, len(seq), size))

class Mapping:
    
    def __init__(self, services_uri, api_key, project_id):
        self.services_uri = f"{services_uri}/mapping/api"
        self._headers = {
            'Authorization': f'Api-Key {api_key}'
        }
        self.project_id = project_id
        print(self._get_project_info())
        
    def _get_project_info(self):
        res = requests.get(
            f"{self.services_uri}/project/{self.project_id}/",
            headers=self._headers,
        )
        utils.handle.check_http_status_code(response=res)
        return res.json()
    
    def add_master_data(self, master_id, text):
        res = requests.post(
            f"{self.services_uri}/master-data/",
            headers=self._headers,
            json={
                "master_id": master_id,
                "text": text,
                "project": self.project_id
            }
        )
        utils.handle.check_http_status_code(response=res)
        return res.json()
    
    def _upload2system(self, df):
        datas = df.to_dict('records')
        res = requests.post(
            f"{self.services_uri}/master-data/bulk-create/",
            headers=self._headers,
            json={
                'bulk': datas
            }
        )
        utils.handle.check_http_status_code(response=res)
        
    def bulk_master_data(self, df, column_id, column_text, n_thred=16):
        df = df[[column_id, column_text]]
        df = df.rename(columns={
           column_id : 'master_id',
           column_text : 'text'
        })
        df['project'] = self.project_id
        ddf  = dd.from_pandas(df, chunksize=100)
        dask.config.set(pool=ThreadPool(n_thred))
        with ProgressBar():
            ddf.map_partitions(self._upload2system).compute()
        res = requests.get(
            f"{self.services_uri}/project/index/",
            headers=self._headers,
        )
        utils.handle.check_http_status_code(response=res)
        return res.json()
    
    @resultify
    def search(self, text, out_list=False):
        if len(text) < 3:
            raise Exception("text less than 2 charaters")
        res = requests.get(
            f"{self.services_uri}/project/{self.project_id}/search/?q={text}",
            headers=self._headers,
        )
        utils.handle.check_http_status_code(response=res)
        out = res.json()
        if not len(out):
            raise Exception("masterdata not found")
        return out if out_list else out[0]
    
class MappingV2:
    
    def __init__(self, services_uri, api_key, master_name):
        self.services_uri = f"{services_uri}/mapping/api"
        self._headers = {
            'Authorization': f'Api-Key {api_key}'
        }
        self.master_name = master_name
        if master_name not in ['country', 'company', 'service', 'product', 'province']:
            raise Exception(f"{master_name} not in system!")
    
    def add_master_data(self, master_id, text):
        res = requests.post(
            f"{self.services_uri}/master-data/",
            headers=self._headers,
            json={
                "master_id": master_id,
                "text": text,
                "project": self.project_id
            }
        )
        utils.handle.check_http_status_code(response=res)
        return res.json()
    
    def _upload2system(self, df):
        datas = df.to_dict('records')
        res = requests.post(
            f"{self.services_uri}/{self.master_name}/bulk-create/",
            headers=self._headers,
            json={
                'bulk': datas
            }
        )
        utils.handle.check_http_status_code(response=res)
        
    def bulk_master_data(self, df, column_id, column_text, n_thred=16):
        df = df[[column_id, column_text]]
        df = df.rename(columns={
           column_id : 'id',
           column_text : 'text'
        })
        ddf  = dd.from_pandas(df, chunksize=1000)
        dask.config.set(pool=ThreadPool(n_thred))
        with ProgressBar():
            ddf.map_partitions(self._upload2system, meta=object).compute()
        res = requests.get(
            f"{self.services_uri}/{self.master_name}/index/",
            headers=self._headers,
        )
        utils.handle.check_http_status_code(response=res)
        return res.json()
    
    @resultify
    def search(self, text, out_list=False):
        if len(text) < 3:
            raise Exception("text less than 2 charaters")
        res = requests.get(
            f"{self.services_uri}/{self.master_name}/search/?q={text}",
            headers=self._headers,
        )
        utils.handle.check_http_status_code(response=res)
        out = res.json()
        if not len(out):
            raise Exception("masterdata not found")
        return out if out_list else out[0]
    
    def query(self, ddf, query_col):
        output = ddf.map_partitions(lambda df: self._query(df=df, query_col=query_col) , meta=tuple).compute()
        out1, out2 = zip(*output)

        ddf = ddf.assign(_key_id = pd.Series(out1))
        ddf = ddf.assign(_key_text = pd.Series(out2))
        return ddf
    
    def _query(self, df, query_col):
        q = df[query_col].tolist()
        res = requests.post(
            f"{self.services_uri}/{self.master_name}/query/",
            headers=self._headers,
            json={
                "bulk": q
            }
        )
        out = res.json()
        return [elm.get('id') for elm in out], [elm.get('text') for elm in out]