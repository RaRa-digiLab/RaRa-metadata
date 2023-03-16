# RaRa-metadata-harvester

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

harvester.harvest(fpath="somefilepath.xml")     # main function - be sure to use the .xml extension
```


