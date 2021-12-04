""" Azure Blog Storage
"""
import logging
import os
import json

from azure.common import AzureHttpError, AzureMissingResourceHttpError
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlockBlobService

logger = logging.getLogger()


class abs():
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
            logger.info('Container "{0}" is successfully finished.'.format(self.container_name))
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
            content = blob.content
            logger.info('{0} is successfully loaded.'.format(file_name))
        except AzureMissingResourceHttpError as e:
            logger.error('There is no flow.json in Azure blob storage.')
            raise SystemExit(e)
        except KeyError as e:
            logger.error('Invalid flow.json format.')
            raise SystemExit(e)

        return content
