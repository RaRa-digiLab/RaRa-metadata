import os
import json
from tqdm import tqdm
import requests
from lxml import etree
from lxml.etree import ElementTree as ET


ns = {"oai": "http://www.openarchives.org/OAI/2.0/",
      "xsi": "http://www.w3.org/2001/XMLSchema-instance"}
for key, value in ns.items():
    etree.register_namespace(key, value)

with open("collections.json", "r", encoding="utf8") as f:
    collections = json.load(f)
            

def update_cursor(token, step):
    """
    Extracts the cursor from the resumptionToken
    """
    token_id, collection, metadata_prefix, cursor, collection_size = token.strip(":").split(":")
    new_cursor = str(int(cursor) + step)
    if int(new_cursor) >= int(collection_size): # reached the last batch
        return None
    else:
        return ":".join([token_id, collection, metadata_prefix, new_cursor, collection_size, ":"])


def request_records(collection_URL=None, token=None):
    """
    Request records of the collection from the server. Made both for initial and follow-up requests.
    """
    # if we don't have a resumptionToken yet, request the first batch; else use the token.
    if token is not None and collection_URL is None:
        URL = f"https://data.digar.ee/repox/OAIHandler?verb=ListRecords&resumptionToken={token}"
    elif collection_URL is not None and token is None:
        URL = collection_URL
    else:
        raise AttributeError("Must provide either a resumptionToken or a collection URL (see harvester.collections for details)")

    response = requests.get(URL)
    tree = ET(etree.fromstring(bytes(response.text, encoding="utf8")))
    root = tree.getroot()
    responseDate, request, ListRecords = root.getchildren()

    # in the case of an initial request, return both the records, resumptionToken and the request metadata
    if token is None:
        try:
            resumptionToken = root.find("./{*}ListRecords/{*}resumptionToken").text
        except AttributeError:
            resumptionToken = None
        # save the request metadata and the token as a class attribute
        request_metadata = {"responseDate": responseDate,
                            "request": request,
                            "resumptionToken": resumptionToken}
        return ListRecords, request_metadata
    # if we already have a resumptionToken, just get the records
    else:
        return ListRecords
    

def get_collection(URL):
    """
    Requests the whole collection in batches using the resumptionToken. Returns all records, as well as the request metadata (header).
    """
    # initial request
    all_records = []
    ListRecords, request_metadata = request_records(collection_URL=URL)
    all_records += ListRecords[:-1]

    token = request_metadata["resumptionToken"]
    if token is not None:
        cursor_step, collection_size = [int(el) for el in token.split(":")[3:5]]
    else:   # token can be none in the case of a small collection that is returned in the initial request
        cursor_step, collection_size = 1000, len(ListRecords)
    #print(f"Fetched {len(ListRecords)} records in first batch from collection with size {collection_size}.\nRequesting rest of the collection...")

    progress_bar = tqdm(total=collection_size, initial=cursor_step)
    while token is not None: # continue requesting until there is no more resumptionToken, i.e. the end of the collection is reached
        ListRecords = request_records(token=token)
        all_records += ListRecords[:-1] # (leave out the last element, the resumptionToken)
        token = update_cursor(token, step=cursor_step) # update the cursor
        progress_bar.update(len(ListRecords)-1)
    progress_bar.close()

    return all_records, request_metadata


def write_start_of_string(metadata):
    """
    Reconstructs the original header of the OAI-PMH request.
    """
    xml_string = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">"""
    xml_string += etree.tostring(metadata["responseDate"],
                                 encoding="utf8",
                                 pretty_print=True).decode()
    xml_string += etree.tostring(metadata["request"],
                                 encoding="utf8",
                                 pretty_print=True).decode()       
    return xml_string


def write_records(ListRecords, metadata, savepath):
    """
    Joins the header and the records into a full XML structure and saves it.
    """
    # if os.path.exists(savepath):
    #     path, extension = savepath.rsplit(".", 1)
    #     savepath = path + "_NEW." + extension
    #     print(f"""The file path already exists. To avoid appending to existing file, data will be saved to:\n '{savepath}'""")
    with open(savepath, "a", encoding="utf8") as f: 
        f.write(write_start_of_string(metadata))
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


def harvest_oai(collection_name, savepath=None):
    """
    Harvests and saves the complete collection in the original OAI-PMH format
    """
    URL = collections[collection_name]
    ListRecords, request_metadata = get_collection(URL=URL)
    #print("Writing file")
    write_records(ListRecords=ListRecords,
                  metadata=request_metadata,
                  savepath=savepath)
    #print("Finished")