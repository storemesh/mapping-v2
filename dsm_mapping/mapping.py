import requests
from . import utils
from .utils.resultify import resultify

class Mapping:
    
    def __init__(self, services_uri, api_key, project_id):
        self.services_uri = f"{services_uri}/mapping/api"
        self._headers = {
            'Authorization': f'Api-Key {api_key}'
        }
        self.project_id = project_id
        self._get_project_info()
        
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
    
    def bulk_master_data(self, df, column_id, column_text):
        df = df[[column_id, column_text]]
        df = df.rename(columns={
           column_id : 'master_id',
           column_text : 'text'
        })
        df['project'] = self.project_id
        
        datas = df.to_dict('records')
        res = requests.post(
            f"{self.services_uri}/master-data/bulk-create/",
            headers=self._headers,
            json={
                'bulk': datas
            }
        )
        utils.handle.check_http_status_code(response=res)
        return res.json()
    
    @resultify
    def search(self, text, out_list=False):
        if len(text) < 2:
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