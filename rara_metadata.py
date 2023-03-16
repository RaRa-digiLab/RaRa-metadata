import os
from lxml import etree
from lxml.etree import ElementTree as ET
import requests
from tqdm import tqdm


class Harvester:

    def __init__(self):

        self.OAI_root_tag = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">"""

        self.ns = {"oai": "http://www.openarchives.org/OAI/2.0/",
            "xsi": "http://www.w3.org/2001/XMLSchema-instance"}

        for key, value in self.ns.items():
            etree.register_namespace(key, value)

        self.collections = {
                "Estonian Legal Biography": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=bie&metadataPrefix=marc21xml",
                "DIGAR - books collection": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=book&metadataPrefix=edm",
                "Digital Archive DIGAR": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=digar&metadataPrefix=edm",
                "DIGAR - EODOPEN collection": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=eodopen&metadataPrefix=marc21xml",
                "ERB - Estonian National Biography": "https://data.digar.ee/repox/OAIHandler?verb=ListRecords&set=erb&metadataPrefix=marc21xml",
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

        self.current_collection = collection_name
        self.current_collection_URL = self.collections[self.current_collection]


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
            resumptionToken = root.find("./{*}ListRecords/{*}resumptionToken").text

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
        cursor_step, collection_size = [int(el) for el in token.split(":")[3:5]]
        
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
    

    def write_records(self, ListRecords: list, fpath=None):

        """Kirjutab kokku kogutud kirjed üheks XML failiks, mis näeb välja selline,
        nagu oleks esimese päringuga kõik kirjed korraga kätte saadud."""

        if os.path.exists(fpath):
            path, extension = fpath.rsplit(".", 1)
            fpath = path + "_NEW." + extension
            print(f"""The file path already exists. To avoid appending to existing file, data will be saved to:\n '{new_fpath}'""")

        with open(fpath, "a", encoding="utf8") as f:
            
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


    def harvest(self, fpath):

        print(f"Collecting {self.current_collection}")
        ListRecords = self.get_collection()
        print("Writing file")
        self.write_records(ListRecords, fpath)
        print("Finished!\n")





