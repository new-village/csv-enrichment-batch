""" main.py
"""
import logging
import sys
from internal import azure_blob_storage, import_data, export_data

__all__ = ['azure_blob_storage', 'import_data', 'export_data']

if __name__ == "__main__":
    # Load logger config & Set Logger
    logging.basicConfig(level='INFO', format='%(asctime)s [%(levelname)s] %(message)s')
    logger = logging.getLogger()
    # Load configuration file from Azure Blob Storage
    flow = azure_blob_storage().load_config()

    # Execute Flow
    for task in flow:
        try:
            data = eval('{0}({1})'.format(task['task'], task))
        except NameError:
            logger.error('Task "{0}" is not defined'.format(task))
            sys.exit(1)

    # 設定ファイル（flow.json）をロード
    # フローを実行
