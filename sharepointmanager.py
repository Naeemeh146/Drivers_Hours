import io
import datetime
import pandas as pd
import xlsxwriter
import logging
from office365_api import SharePoint

logging.basicConfig(filename='app.log', level=logging.INFO)

class SharePointManager:
    def __init__(self, read, folder_name, dest_folder_name, df_processed=None):
        self.read = read
        self.folder_name = folder_name
        self.dest_folder_name = dest_folder_name
        self.df_processed = df_processed

    def take_weekday_name(self):
        today = datetime.date.today()
        weekday_index = today.weekday()
        weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        return weekdays[weekday_index]

    def just_write(self):
        logging.info("Started Process for writing")
        output_obj = io.StringIO()
        
        # Write DataFrame to CSV
        self.df_processed.to_csv(output_obj, index=False)

        output_obj.seek(0)
        csv_bytes = output_obj.getvalue().encode('utf-8')

        today_date = datetime.datetime.today().strftime('%Y-%m-%d')
        name = 'Output-' + today_date + '.csv'
        logging.info(f"Generated file name: {name}")
        
        # Upload the CSV file
        self.upload_file(name, self.dest_folder_name, csv_bytes)

    def just_read(self, file_obj):
        try:
            logging.info("Started reading process")
            data = io.BytesIO(file_obj)
            xls = pd.ExcelFile(data, engine='openpyxl')
            ws_list = xls.sheet_names
            logging.info(f"Sheet names: {ws_list}")

            weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            df_lineup = None
            dayname = []
            for weekday_name in weekday_names:
                if weekday_name in ws_list:
                    dayname.append(weekday_name)
                    try:
                        df_lineup = pd.read_excel(xls, sheet_name=weekday_name)
                        break
                    except Exception as e:
                        logging.error(f"Error reading sheet {weekday_name}: {str(e)}")

            if df_lineup is None:
                logging.error("No valid weekday sheet found.")
                return None, None

            if 'DOW' not in ws_list:
                logging.error(f"'DOW' sheet not found. Available sheets: {ws_list}")
                return None, None

            try:
                df_dow = pd.read_excel(xls, sheet_name='DOW', skiprows=1)
                return df_lineup, df_dow,dayname
            except Exception as e:
                logging.error(f"Error reading 'DOW' sheet: {str(e)}")
                return None, None

        except Exception as e:
            logging.error(f"Error reading file: {str(e)}")
            return None, None

    def upload_file(self, file_name, folder_dest, content):
        try:
            logging.info(f"Uploading to folder: {folder_dest}")
            SharePoint().upload_file(file_name, folder_dest, content)
            logging.info(f"Uploaded file: {file_name}")
        except Exception as e:
            logging.error(f"Error uploading file '{file_name}': {str(e)}")

    def upload_log_file_to_sharepoint(self, log_file_name, folder_dest):
        try:

            self.upload_file(log_file_name, folder_dest, logging)
            logging.info(f"Log file {log_file_name} uploaded to {folder_dest}")
        except Exception as e:
            logging.error(f"Error uploading log file '{log_file_name}': {str(e)}")

    def get_file(self, file_n):
        try:
            file_obj = SharePoint().download_file(file_n, self.folder_name)
            if self.read:
                df_lineup, df_dow, dayname = self.just_read(file_obj)
                if df_lineup is None or df_dow is None:
                    logging.error("Failed to read one or both DataFrames.")
                    return None, None
                logging.info("Returning df_lineup and df_dow")
                return df_lineup, df_dow,dayname
        except Exception as e:
            logging.error(f"Error getting file: {str(e)}")
            return None, None

    def get_files(self):
        try:
            files_list = SharePoint()._get_files_list(self.folder_name)
            if self.read:
                df_lineup, df_dow, dayname = self.get_file(files_list[0])
                if df_lineup is None or df_dow is None:
                    logging.error("Failed to process file.")
                    return None, None
                return df_lineup, df_dow, dayname
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
