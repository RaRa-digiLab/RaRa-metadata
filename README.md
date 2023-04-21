# RaRa metadata harvester

### Installing the package and requirements
```
git clone https://github.com/krkryger/RaRa-metadata.git
pip install -r requirements.txt
```

### Harvesting metadata from the OAI-PMH endpoint
```
from harvester import collections, harvest_oai

# see the available datasets and choose one to download
print(collections)
>>> {'Estonian Legal Bibliography': 'https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=bie&metadataPrefix=marc21xml',
     'DIGAR - books collection': 'https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=book&metadataPrefix=edm',
     'DIGAR - maps collection': 'https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=map&metadataPrefix=edm',
     ...
 
harvest_oai(collection_name="DIGAR - books collection",
            savepath="digar_books.xml")
```

### Converting downloaded files from XML to DataFrame/dict/JSON
```
from converter import oai_to_dataframe, oai_to_dict, oai_to_json

# convert to DataFrame and save as TSV
df = oai_to_dataframe(filepath="digar_books.xml")
df.to_csv("digar_books.tsv", sep="\t", encoding="utf8", index=False)

# convert to dictionary
records_as_dict = oai_to_dict(filepath="digar_books.xml")

# or save directly as JSON
oai_to_json(filepath="digar_books.xml",
            json_output_path="digar_books.json")
```

When converting MARC21XML files to a dataframe, the columns that are mostly empty will be dropped automatically. This can be modified with the ```marc_threshold``` parameter in the ```oai_to_dataframe``` function (the default value ````0.1``` means that columns with > 90% NA values are dropped). Coverting to dict or JSON keeps all fields.
