# mapping-v2

- install package
```
pip install -e https://github.com/storemesh/mapping-v2.git
```
- declare instance
```python
import dsm_mapping
import pandas as pd

mapping = dsm_mapping.mapping.Mapping(
    services_uri='<services_uri>',
    api_key="<api_key>",
    project_id=<project_id>
)
```

- add one
```python
mapping.add_master_data(master_id="glass01", text="แก้วน้ำ")
```

- search
    - search return list
    ```python
    mapping.search(text="แก้วน้")
    ```
    - search return dict
    ```python
    mapping.search(text="แก้วน้", out_list=False)
    ```

- bulk create via dataframe

```python
df = pd.read_csv('<data path>')
mapping.bulk_master_data(df=df, column_id='<column_id>', column_text='<column_text>')
```