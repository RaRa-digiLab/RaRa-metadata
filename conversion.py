import re
from lxml import etree
import pandas as pd


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
    """Detects whether a parsed XML tree is in OAI-PMH or EDM format"""
    ns = get_namespaces()

    if tree.find("./oai:ListRecords/oai:record/oai:metadata/marc:*", namespaces=ns) is not None:
        return "marc"
    elif tree.find("./oai:ListRecords/oai:record/oai:metadata/rdf:RDF/edm:*", namespaces=ns) is not None:
        return "edm"
    else:
        raise ValueError("Cannot determine data format. The OAI-PMH ListRecords response must be made up of either EDM or MARC21XML records.")


def detect_record_format(record):
    """Detects whether a single record is in OAI-PMH or EDM format"""
    ns = get_namespaces()

    if record.find("./oai:metadata/marc:*", namespaces=ns) is not None:
        return "marc"
    elif record.find("./oai:metadata/rdf:RDF/edm:*", namespaces=ns) is not None:
        return "edm"
    else:
        raise ValueError("Cannot determine data format. The OAI-PMH ListRecords response must be made up of either EDM or MARC21XML records.")


def extract_year(date):
    """
    Cleans a datetime string to find a valid year.
    """
    
    if len(date) == 4 and date.isnumeric():
        if int(date) > 1500 and int(date) < 2024:
            return int(date)
        else:
            return None

    patterns = [re.compile("(^([\D\s]+)(\d{4})([\D\s]*)$)|(^([\D\s]*)(\d{4})([\D\s]+)$)"),
                re.compile("^\d{4}-\d{2}-\d{2}$"),
                re.compile("^\d{2}-\d{2}-\d{4}$"),
                re.compile("^\d{4}-\d{2}$")]

    for pattern in patterns:
        if re.match(pattern, date):
            date = re.findall("\d{4}", date)[0]

    if len(date) == 4:
        try:
            date = int(date)
            if date > 1500 and date < 2024:
                return date
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