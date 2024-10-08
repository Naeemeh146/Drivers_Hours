import io
import datetime
import pandas as pd
import xlsxwriter
import logging
from office365_api import SharePoint

logging.basicConfig(filename='app.log', level=logging.INFO)

class TimeSharePointManager:
    def __init__(self, read, dayname ,folder_name, df_processed=None):
        self.read = read
        self.folder_name = folder_name
        self.df_processed = df_processed
        self.dayname = dayname





    def just_read(self, file_obj):
        try:
            logging.info("Started reading process")
            data = io.BytesIO(file_obj)
            xls = pd.ExcelFile(data, engine='openpyxl')
            df_time_all = pd.read_excel(xls)
            print(df_time_all)
            df_time = df_time_all[df_time_all['Day'] == self.dayname[0]]
            return df_time
        except Exception as e:
            logging.error(f"Error reading file: {str(e)}")
            return None, None

    

    def get_file(self, file_n):
        try:
            file_obj = SharePoint().download_file(file_n, self.folder_name)
            #print(file_obj)
            print("I am here")
            if self.read:
                df_time = self.just_read(file_obj)
                if df_time is None:
                    logging.error("Failed to read one or both DataFrames.")
                    return None, None
                logging.info("Returning df_lineup and df_dow")
                return df_time
        except Exception as e:
            logging.error(f"Error getting file: {str(e)}")
            return None, None

    def get_files(self):
        try:
            files_list = SharePoint()._get_files_list(self.folder_name)
            if self.read:
                df_time = self.get_file(files_list[0])
                if df_time is None:
                    logging.error("Failed to process file.")
                    return None, None
                return df_time
            else:
                self.just_write()
                logging.info('File has been uploaded properly')
        except Exception as e:
            logging.error(f"Error getting files: {str(e)}")
            return None, None

    def process_files(self):
        if self.read:
            logging.info("Started reading file from SharePoint")
            df = self.get_files()
            if df is None:
                logging.error("Failed to read files.")
            return df
        else:
            logging.info("Started uploading file to SharePoint")
            self.get_files()
