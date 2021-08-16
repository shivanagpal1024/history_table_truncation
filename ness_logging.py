import requests
import json
import socket
from logger import get_logger


class NessLogging:

    def __init__(self, url=None):
        self.log = get_logger(__name__)
        if url is None or len(url) == 0:
            raise ValueError(f"url={url} is None or empty")
        self.__url = url

    def post(self, **config):
        if config is None or len(config) != 5:
            msg = f"config={config} is None or some values are missing"
            self.log.error(msg)
            raise ValueError(msg)

        operation = config['operation']
        del config['operation']
        config['logClass'] = "NONSECURITY"
        config['application'] = {"askId": "UHGWM110-000154", "name": "ecap"}
        config['device'] = {"vendor": "Optum", "product": "History Table Truncation", "hostname": socket.gethostname()}
        config['fileRecord'] = {"name": config['name'], "operation": str(operation)}

        json_string = json.dumps(config)
        headers = {'Content-Type': 'application/json; charset=UTF-8'}
        #self.log.info(f'json string - {json_string}')
        #self.log.info(f'headers - {headers}')        
        x = requests.post(self.__url, data=json_string, headers=headers)
        if x.status_code != 200:
            self.log.error(f'sending log error to NESS failed with {x.status_code}')
        else:
            self.log.info(f'log error delivered to NESS successfully')

        x.close()

