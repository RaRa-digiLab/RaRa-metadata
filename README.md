# RaRa metadata harvester

### Installing the package and requirements
```
git clone https://github.com/krkryger/RaRa-metadata-harvester.git
pip install -r requirements.txt
```

### Usage

#### Import and initialize the harvester
```
from rara_metadata import Harvester

harvester = Harvester()
```

#### See the available datasets and choose one to download
```
harvester.collections     # returns a dictionary with names and URLs

>>> {'Estonian Legal Biography': 'https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=bie&metadataPrefix=marc21xml',
     'DIGAR - books collection': 'https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=book&metadataPrefix=edm',
     'DIGAR - maps collection': 'https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=map&metadataPrefix=edm',
     ...
     
harvester.set_collection(collection_name="DIGAR - maps collection")     # this is one of the smaller ones

harvester.harvest(savepath="somefilepath.xml")     # main function - be sure to use the .xml extension
```

#### Convert EDM to DataFrame
The metadata files can be either in MARC21XML or EDM (Europeana Data Model) specification. For now, there is a functionality to convert the latter into DataFrame which can then be saved in a preferred format:
```
from lxml import etree
from rara_metadata import detect_format, edm_to_dataframe

tree = etree.parse(source="somefilepath.xml")

if detect_format(tree) == "edm":
     df = edm_to_dataframe(source=tree)
     df.to_csv("somesavepath.tsv", sep="\t", encoding="utf8", index=False)
```

Alternatively, if you already know that the file is in EDM format, there is no need for verification:
```
from lxml import etree
from rara_metadata import detect_format, edm_to_dataframe

df = edm_to_dataframe(source="somefilepath.xml)
df.to_csv("somesavepath.tsv", sep="\t", encoding="utf8", index=False)
```
