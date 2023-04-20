import os
import json
from tqdm import tqdm
import requests
import pandas as pd
from lxml import etree
from lxml.etree import ElementTree as ET
from conversion import extract_edm_metadata, detect_record_format


class Harvester:

    def __init__(self):
        self.OAI_root_tag = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">"""
        self.ns = {"oai": "http://www.openarchives.org/OAI/2.0/",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance"}

        for key, value in self.ns.items():
            etree.register_namespace(key, value)

        with open("collections.json", "r", encoding="utf8") as f:
            self.collections = json.load(f)
            
        
    def set_collection(self, collection_name):
        try:       
            self.current_collection_URL = self.collections[collection_name]
            self.current_collection = collection_name
        except KeyError:
            print("Invalid collection name. Valid names are:")
            print(list(self.collections.keys())) 


    def update_cursor(self, token, step):
        """
        Extracts the cursor from the resumptionToken
        """
        token_id, collection, metadata_prefix, cursor, collection_size = token.strip(":").split(":")
        new_cursor = str(int(cursor) + step)

        if int(new_cursor) >= int(collection_size): # reached the last batch
            return None
        else:
            return ":".join([token_id, collection, metadata_prefix, new_cursor, collection_size, ":"])


    def request_records(self, token):
        """
        Request records of the collection from the server. Made both for initial and follow-up requests.
        """
        # if we don't have a resumptionToken yet, request the first batch; else use the token.
        if token == "first-request":
            URL = self.current_collection_URL
        else:
            URL = f"https://data.digar.ee/repox/OAIHandler?verb=ListRecords&resumptionToken={token}"

        response = requests.get(URL)
        tree = ET(etree.fromstring(bytes(response.text, encoding="utf8")))
        root = tree.getroot()
        responseDate, request, ListRecords = root.getchildren()

        if token == "first-request":
            # in the case of an initial request, return both the records, resumptionToken and the request metadata
            try:
                resumptionToken = root.find("./{*}ListRecords/{*}resumptionToken").text
            except AttributeError:
                resumptionToken = None
            # save the request metadata and the token as a class attribute
            self.metadata = {"responseDate": responseDate,
                             "request": request,
                            "resumptionToken": resumptionToken}
            return ListRecords
        else:
            # if we already have a resumptionToken, just get the records
            return ListRecords
        

    def detect_collection_format(self):
        """
        Detects the format of a collection from the first batch of records returned.
        """
        ListRecords = self.request_records(token="first-request")
        return detect_record_format(ListRecords[0])


    def get_collection(self):
        """
        Requests the whole collection in batches using the resumptionToken. Returns all records, as well as the request metadata (header).
        """
        # initial request
        all_records = []
        ListRecords = self.request_records(token="first-request")
        all_records += ListRecords[:-1]

        token = self.metadata["resumptionToken"]
        if token is not None:
            cursor_step, collection_size = [int(el) for el in token.split(":")[3:5]]
        else:   # token can be none in the case of a small collection that is returned in the first request
            cursor_step, collection_size = 1000, len(ListRecords)
        print(f"Fetched {len(ListRecords)} records in first batch from collection with size {collection_size}.\nRequesting rest of the collection...")

        progress_bar = tqdm(total=collection_size, initial=cursor_step)
        while token is not None: # continue requesting until there is no more resumptionToken, i.e. the end of the collection is reached
            ListRecords = self.request_records(token=token)
            all_records += ListRecords[:-1] # (leave out the last element, the resumptionToken)
            token = self.update_cursor(token, step=cursor_step) # update the cursor
            progress_bar.update(len(ListRecords)-1)
        progress_bar.close()

        return all_records
    

    def write_start_of_string(self):
        """
        Reconstructs the original header of the OAI-PMH request.
        """
        xml_string = self.OAI_root_tag
        xml_string += etree.tostring(self.metadata["responseDate"],
                                     encoding="utf8",
                                     pretty_print=True,
                                     ).decode()
        xml_string += etree.tostring(self.metadata["request"],
                                     encoding="utf8",
                                     pretty_print=True,
                                     ).decode()       
        return xml_string
    

    def write_records(self, ListRecords: list, savepath=None):
        """
        Joins the header and the records into a full XML structure and saves it.
        """
        if os.path.exists(savepath):
            path, extension = savepath.rsplit(".", 1)
            savepath = path + "_NEW." + extension
            print(f"""The file path already exists. To avoid appending to existing file, data will be saved to:\n '{savepath}'""")

        with open(savepath, "a", encoding="utf8") as f: 
            f.write(self.write_start_of_string())
            f.write("<ListRecords>")
            for entry in ListRecords:
                entry_as_xml_tree = ET(entry)
                entry_as_string = etree.tostring(entry_as_xml_tree,
                                                encoding="utf8",
                                                pretty_print=True,
                                                ).decode()
                f.write(entry_as_string)
            f.write("</ListRecords>")
            f.write("</OAI-PMH>")


    def harvest(self, format, savepath=None):
        """
        Harvests the complete collection and either saves it in the original OAI-PMH format or returns either a serializable dict or a pandas DataFrame.
        
        Parameters
        ----------
        format : string
            How you would like to receive the collection. Possible values are the following:
            - "oai-pmh" : saves the collection in the original OAI-PMH XML format. Parameter savepath required.
            - "json" : returns the collection as a serializable dictionary.
            - "dataframe" : returns the collection as a pandas DataFrame object.
        savepath : string (optional)
            Savepath is needed only if "format" is set to "oai-pmh".
        """

        if format not in ["oai-pmh", "json", "dataframe"]:
            raise ValueError("Invalid format specification. Possible values are: 'oai-pmh', 'json', 'dataframe'")
        if format == "oai-pmh" and savepath is None:
            raise ValueError("Harvesting as OAI-PMH requires a valid savepath")
        
        collection_format = self.detect_collection_format()
        print(f"Collecting {self.current_collection}")

        if format == "oai-pmh":
            ListRecords = self.get_collection()
            print("Writing file")
            self.write_records(ListRecords, savepath)
            print("Finished")

        elif format == "json":
            if collection_format == "edm":
                ListRecords = self.get_collection()
                records_as_json = {"records": []}
                for record in (extract_edm_metadata(record) for record in ListRecords):
                    records_as_json["records"].append(record)
                return records_as_json
            elif collection_format == "marc":
                print("Sorry, this collection is in MARC format and can currently only be returned as an XML file. Use format='oai-pmh'.")
                return None
        
        elif format ==  "dataframe":
            if collection_format == "edm":
                ListRecords = self.get_collection()
                records_metadata = (extract_edm_metadata(record) for record in ListRecords)
                records_as_df = pd.DataFrame.from_records(records_metadata).convert_dtypes()
                return records_as_df
            elif collection_format == "marc":
                print("Sorry, this collection is in MARC format and can currently only be returned as an XML file. Use format='oai-pmh'.")
                return None