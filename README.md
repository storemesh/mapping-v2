# mapping-v2

## Mapping
- install package
```
pip install -U git+https://github.com/storemesh/mapping-v2.git
```
- declare instance
```python
import dsm_mapping
import pandas as pd

mapping = dsm_mapping.mapping.MappingV2(
    services_uri='<services_uri>',
    api_key="<api_key>",
    master_name='<master_name>'
)
```
- master choices 
    ```
    [ 'country', 'company', 'service', 'product', 'province']
    ```

- add one
```python
mapping.add_master_data(master_id="glass01", text="แก้วน้ำ")
```

- search
```python
mapping.search(text="แก้วน้")
```


- bulk create via dataframe

```python
df = pd.read_csv('<data path>')
mapping.bulk_master_data(df=df, column_id='<column_id>', column_text='<column_text>')
```

## MDM

```
pip install git+https://github.com/storemesh/mapping-v2.git#egg=dsm_mapping[mdm]
```