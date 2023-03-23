import os
from lxml import etree
from lxml.etree import ElementTree as ET
import requests
from tqdm import tqdm
import pandas as pd
import re


class Harvester:

    def __init__(self):
        self.OAI_root_tag = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">"""

        self.ns = {"oai": "http://www.openarchives.org/OAI/2.0/",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance"}

        for key, value in self.ns.items():
            etree.register_namespace(key, value)

        self.collections = {
                "Estonian Legal Bibliography": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=bie&metadataPrefix=marc21xml",
                "DIGAR - books collection": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=book&metadataPrefix=edm",
                "Digital Archive DIGAR": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=digar&metadataPrefix=edm",
                "DIGAR - EODOPEN collection": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=eodopen&metadataPrefix=marc21xml",
                "ERB - Estonian National Bibliography": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=erb&metadataPrefix=marc21xml",
                "ERB - sound recordings": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=helisalvestised&metadataPrefix=marc21xml",
                "DIGAR - journals collection": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=journal&metadataPrefix=edm",
                "ERB - maps": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=kaardid&metadataPrefix=marc21xml",
                "DIGAR - maps collection": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=map&metadataPrefix=edm",
                "ERB - multimedia": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=multimeedia&metadataPrefix=marc21xml",
                "ERB - books in foreign language": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=muukeelne&metadataPrefix=marc21xml",
                "ERB - sheet music": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=noodid&metadataPrefix=marc21xml",
                "Organization names": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=organization&metadataPrefix=marc21xml"
            }
        
    def set_collection(self, collection_name):
        try:       
            self.current_collection_URL = self.collections[collection_name]
            self.current_collection = collection_name
        except KeyError:
            print("Invalid collection name. Valid names are:")
            print(list(self.collections.keys())) 


    def update_cursor(self, token, step=250):
        """Eraldab tokenist kursori ehk selle, mitmenda kirje juurde päring jäi, ning konstrueerib selle abil uue tokeni"""

        token_id, collection, metadata_prefix, cursor, collection_size = token.strip(":").split(":")
        new_cursor = str(int(cursor) + step)

        if int(new_cursor) >= int(collection_size): # reached the last batch
            return None
        else:
            return ":".join([token_id, collection, metadata_prefix, new_cursor, collection_size, ":"])


    def request_records(self, token):
        """Keskne funktsioon serverist päringu tegemiseks.
        On loodud nii esmase päringu kui ka tokeniga järelpäringu tegemiseks."""

        # otsustab, kas tegu on esmase või järelpäringuga
        if token == "first request":
            URL = self.current_collection_URL
        else:
            URL = f"https://data.digar.ee/repox/OAIHandler?verb=ListRecords&resumptionToken={token}"

        # päring ise
        response = requests.get(URL)

        # päringu tulemus serialiseeritakse XML puuks
        tree = ET(etree.fromstring(bytes(response.text, encoding="utf8")))
        root = tree.getroot()
        responseDate, request, ListRecords = root.getchildren()

        if token == "first request":
            # esmase päringu puhul otsib üles tokeni ja salvetab päringu metaandmed,
            # et neid hiljem lõpliku faili koostamisel kasutada
            try:
                resumptionToken = root.find("./{*}ListRecords/{*}resumptionToken").text
            except AttributeError:
                resumptionToken = None
            self.metadata = {"responseDate": responseDate,
                             "request": request,
                            "resumptionToken": resumptionToken}
            return ListRecords
        else:
            # järelpäringu puhul on vaja ainult kirjeid endid
            return ListRecords
        

    def get_collection(self):

        """Funktsioon, mis teeb soovitud andmestikust kõigepealt esmase päringu ning kasutab siis tokenit järelpäringute tegemiseks,
        kuni terve andmestik on alla laetud. Tagastab kõik kirjed ja päringu metaandmed."""

        all_records = []

        # esmane päring
        ListRecords = self.request_records(token="first request")
        all_records += ListRecords

        # tokenist saame teada kursori sammu (mitu kirjet korraga antakse) ja andmestiku kogusuuruse
        token = self.metadata["resumptionToken"]

        if token is not None:
            cursor_step, collection_size = [int(el) for el in token.split(":")[3:5]]
        else:
            collection_size = len(ListRecords)
        print(f"Fetched {len(ListRecords)} records in first batch from collection with size {collection_size}.\nRequesting rest of the collection...")

        progress_bar = tqdm(total=collection_size)
        while token is not None:
            # järelpäring
            ListRecords = self.request_records(token=token)
            all_records += ListRecords
            # tokeni uuendamine
            token = self.update_cursor(token, step=cursor_step)
            progress_bar.update(cursor_step)
        progress_bar.close()

        return all_records[:-1] # (jätame viimase elemendi välja, sest see on resumptionToken)
    

    def write_start_of_string(self):
        """Taastab algse päringu alguse, kus on kirjas päringu metaandmed, ning paigutab sõne algusse OAI juure."""
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
        """Kirjutab kokku kogutud kirjed üheks XML failiks, mis näeb välja selline,
        nagu oleks esimese päringuga kõik kirjed korraga kätte saadud."""
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


    def harvest(self, savepath):
        print(f"Collecting {self.current_collection}")
        ListRecords = self.get_collection()
        print("Writing file")
        self.write_records(ListRecords, savepath)
        print("Finished!\n")


def get_namespaces():
    return {"xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "oai": "http://www.openarchives.org/OAI/2.0/",
            "marc": "http://www.loc.gov/MARC21/slim",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "edm": "http://www.europeana.eu/schemas/edm/",
            "dc" : "http://purl.org/dc/elements/1.1/"}


def register_namespaces():
    for key, value in get_namespaces().items():
        etree.register_namespace(key, value)


def detect_format(tree):
    """Detects whether a parsed XML tree is in XML or EDM format"""
    ns = get_namespaces()

    if tree.find("./oai:ListRecords/oai:record/oai:metadata/marc:*", namespaces=ns) is not None:
        return "marc"
    elif tree.find("./oai:ListRecords/oai:record/oai:metadata/rdf:RDF/edm:*", namespaces=ns) is not None:
        return "edm"
    else:
        raise ValueError("Cannot determine data format. The OAI-PMH ListRecords response must be made up of either EDM or MARC21XML records.")


def extract_year(date):
    if len(date) == 4 & date.isnumeric():
        return int(date)

    patterns = [re.compile("(^([\D\s]+)(\d{4})([\D\s]*)$)|(^([\D\s]*)(\d{4})([\D\s]+)$)"),
                re.compile("^\d{4}-\d{2}-\d{2}$"),
                re.compile("^\d{2}-\d{2}-\d{4}$"),
                re.compile("^\d{4}-\d{2}$")]

    for pattern in patterns:
        if re.match(pattern, date):
            date = re.findall("\d{4}", date)[0]

    if len(date) == 4:
        try:
            return int(date)
        except ValueError:
            return None
    else:
        return None
    

def extract_edm_metadata(record, sep="; "):
    """Converts a single EDM record to a dictionary"""

    fields = record.iterfind("./oai:metadata/rdf:RDF/edm:ProvidedCHO/dc:*", namespaces=get_namespaces())
    record_metadata = {}

    for f in fields:
        tag = f.tag.rsplit("}", 1)[1]
        lang = f.attrib.get("{http://www.w3.org/XML/1998/namespace}lang")
        text = f.text

        if tag == "identifier":
            if ":isbn:" in text:
                tag = "isbn"
            elif "www.ester.ee" in text:
                tag = "ester_url"
            elif "www.digar.ee" in text:
                tag = "digar_url"
            else:
                tag = "other_identifier"

        if tag == "date":
            record_metadata["year"] = extract_year(text)        
        if lang is not None:
            tag = tag + "_" + lang 
        if tag in record_metadata.keys():
            record_metadata[tag] += sep + text
        else:
            record_metadata[tag] = text

    return record_metadata


def get_records(source):
    """Parses the records of an EDM tree and returns the record objects.
    Input: filepath or lxml.etree._ElementTree object
    Output: list"""

    if type(source) == str:
        if source.lower().endswith(".xml"):
            tree = etree.parse(source)
        else:
            raise ValueError("Invalid path to file. Must be in .xml format.")
    elif type(source) == etree._ElementTree:
        tree = source
    else:
        raise ValueError("Source must be either path to XML file or lxml.etree._ElementTree")

    register_namespaces()

    root = tree.getroot()
    records = root.findall("./oai:ListRecords/oai:record", namespaces=get_namespaces())

    return records


def edm_to_json(source):
    """Parses the records of an EDM tree and returns the records as dictionary.
    Input: filepath or lxml.etree._ElementTree object
    Output: dict"""

    records = get_records(source)
    records_as_json = {"records": []}

    for record in (extract_edm_metadata(record) for record in records):
        records_as_json["records"].append(record)

    return records_as_json


def edm_to_dataframe(source):
    """Parses the records of an EDM tree and returns the records as a dataframe.
    Input: filepath or lxml.etree._ElementTree object
    Output: pandas.DataFrame"""

    records = get_records(source)
    records_metadata = (extract_edm_metadata(record) for record in records)

    return pd.DataFrame.from_records(records_metadata).convert_dtypes()