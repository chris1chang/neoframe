# neoframe

Given a pandas dataframe in a form of an edgelist, neoframe lets you ingest data into neo4j by setting up a simple schema and a connection to a neo4j dbms

Example:

```
import pandas as pd
from neoframe import NeoFrame

df = pd.DataFrame({'name': ['Alice', 'Bob', 'Carol'], 
'age': ['55','66','77'], 
'city': ['DC', 'San Francisco', 'Houston']})

nodes = [('Person', 'name'), ('City', 'city')] # (Label, column)
node_attributes = {'Person':['age']} # attributes per node

edges = {('Person','City'): 'LIVES_IN'} #define edge label

g = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

neoFrame = NeoFrame(graph=g, dataframe=df, nodes=nodes, node_attributes=node_attributes, edges=edges)
neoFrame.create_graph()
```

This creates a neo4j graph of 
