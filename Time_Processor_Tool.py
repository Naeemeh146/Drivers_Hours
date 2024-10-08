import os
import pandas as pd
from datetime import datetime
import logging
import openpyxl


class TimeProcessor:
    def __init__(self, df_dow):
        self.df = df_dow
        
        logging.basicConfig(level=logging.INFO)


    def clean_columns(self):
        df_copy = self.df
        df = df_copy.drop(df_copy.filter(regex='Unnamed').columns, axis=1)
        df.columns = df.columns.astype(str)
        filtered_list = list(df.columns)
        return df , filtered_list


    def find_indexes(self, filtered_list):
        indexes_with_start = [index for index, item in enumerate(filtered_list) if "Start" in item]
        indexes_with_Name = [i - 1 for i in indexes_with_start]
        indexes_with_AM = [i + 1 for i in indexes_with_start]
        return indexes_with_start, indexes_with_Name, indexes_with_AM


    def start_time_extract(self, df, filtered_list, indexes_with_AM):
        Route_manager = []
        Start_time = []
        #print(df.head(5))
        for index, row in df.iterrows():
            if index == 0:
                for j in indexes_with_AM:
                    Start_time.append(filtered_list[j])
                    Route_manager.append(filtered_list[j - 2])

        dictionary_start_time = {'Route': Route_manager, 'Driver str': Start_time}
        df_time = pd.DataFrame(dictionary_start_time)
        exclude_list1 = ['Not Number', '0', 'nan', 'TRUCK'] + [str(i) for i in range(0, 21)]
        df_time = df_time[~df_time['Route'].isin(exclude_list1)]
        return df_time


    @staticmethod
    def clean_time(row):
        """Clean up time data."""
        row_clean = str(row).split('.')[0]
        return row_clean


    def process_xlsm_files(self):
        """Process Excel files in the given directory."""
        try:
            #df = pd.read_excel(os.path.join(self.directory, file), 'DOW', skiprows=1, engine='openpyxl')

            # Data cleaning and manipulation steps...
            # 1- Cleaning columns
            df, filtered_list = self.clean_columns()
            

            # 2- Find indexes including targeted data
            indexes_with_start, indexes_with_Name, indexes_with_AM = self.find_indexes(filtered_list)
            

            # 3- Preprocessing and creating time dataframe
            df_time = self.start_time_extract(df, filtered_list, indexes_with_AM)
            

            # 4- Clean time column
            df_time['Driver str'] = df_time['Driver str'].apply(self.clean_time)

            # 5- Formating time column
            df_time['Driver'] = pd.to_datetime(df_time['Driver str'], format='%I:%M%p').dt.time
            df_time.drop(columns=['Driver str'], inplace=True)

            # 6- Calculating 'Helper' time
            df_time['Helper'] = pd.to_datetime(df_time['Driver'], format='%H:%M:%S') + pd.Timedelta(minutes=15)
            df_time['Helper'] = df_time['Helper'].dt.time
            print("Processed time data")
            logging.info(f"Processed time data")
            df_time.loc[df_time.index[-1], 'Route'] = 'Mix ' + df_time.loc[df_time.index[-1], 'Route']
            #print(df_time)
            return df_time

        except Exception as e:
            logging.error(f"Error processing file Error: {e}")



