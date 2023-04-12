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
```

#### Harvest the collection metadata
##### pandas DataFrame
```
records_dataframe = harvester.harvest(format="dataframe")
```
##### as JSON
```
records_as_json = harvester.harvest(format="json")
```
##### OAI-PMH (original)
When harvesting as OAI-PMH, the records are stored directly in a file and a savepath must therefore be provided.
```
harvester.harvest(format="oai-pmh", savepath="somefilepath.xml")     # be sure to use the .xml extension
```
