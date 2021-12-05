""" Azure Blog Storage
"""
import logging
import os
import json
import io

from azure.common import AzureHttpError, AzureMissingResourceHttpError
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlockBlobService

logger = logging.getLogger()


class azure_blob_storage():
    def __init__(self):
        # Application Define Variables: Container Name
        self.container_name = 'csv-enrichment-batch'

        # Create the client
        # https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blockblobservice.blockblobservice?view=azure-python-previous
        try:
            account = os.environ['STORAGE_ACCOUNT']
            key = os.environ['STORAGE_KEY1']
            self.service = BlockBlobService(account_name=account, account_key=key)
        except KeyError as e:
            logger.error('There is no expected environment variables.')
            raise SystemExit(e)

        # Create Container
        try:
            self.service.create_container(self.container_name)
            logger.info('Container "{0}" is successfully created.'.format(self.container_name))
        except AzureHttpError as e:
            logger.error('The container_name should be contain lower letters: ' + self.container_name)
            raise SystemExit(e)
        except ResourceExistsError:
            logger.info(self.container_name + ' is already exists.')
            pass

    def load_config(self):
        # Get blob data from Azure blob storage
        try:
            blob = self.service.get_blob_to_text(self.container_name, 'flow.json')
            config = json.loads(blob.content)['flow']
            logger.info('flow.json is successfully loaded.')
        except AzureMissingResourceHttpError as e:
            logger.error('There is no flow.json in Azure blob storage.')
            raise SystemExit(e)
        except KeyError as e:
            logger.error('Invalid flow.json format.')
            raise SystemExit(e)

        return config

    def load_csv(self, file_name):
        # Get blob data from Azure blob storage
        try:
            blob = self.service.get_blob_to_text(self.container_name, file_name)
            logger.info('{0} by UTF8 is successfully loaded'.format(file_name))
        except UnicodeDecodeError:
            blob = self.service.get_blob_to_text(self.container_name, file_name, encoding="cp932")
            logger.info('{0} by CP932 is successfully loaded'.format(file_name))
        except AzureMissingResourceHttpError as e:
            logger.error('There is no {0} in Azure blob storage.'.format(file_name))
            raise SystemExit(e)

        # Convert BLOB to StringIO
        content = io.StringIO(blob.content)

        return content
