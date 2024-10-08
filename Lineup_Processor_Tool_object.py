import pandas as pd
import os
from datetime import datetime
import logging


class LineupProcessor:
    def __init__(self, df_lineup, df_time):
        self.df_lineup = df_lineup
        self.df_time = df_time
        logging.basicConfig(level=logging.INFO)


    def setup_logging(self):
        # Set up logging configuration
        logging.basicConfig(level=logging.INFO)

    def each_table_keys(self, table):
        dictionary_keys = []
        for j, row in table.iterrows():
            if j==0 :
                dictionary_keys.append('Type')
                dictionary_keys.append(row['Unnamed: 2'])
                dictionary_keys.append(row['Unnamed: 3'])
                dictionary_keys.append('Driver')
                dictionary_keys.append('Details')
                dictionary_keys.append('Tablet #')
                dictionary_keys.append('Second Note')
                dictionary_keys.append('Comments')

            else:
                pass
        
        table_dict = {}
        for key in dictionary_keys:
            table_dict[key] = []
    
        return dictionary_keys, table_dict

    def each_table_data(self, indices, dictionary_keys, table_dict, table):
        # Iterate over the indices and extract data
        for i in range(len(indices)-1):
            start_index = indices[i]
            end_index = indices[i+1]

            temp_df = table.iloc[start_index:end_index]

            # about the start index need to put attention!!!!!
            temp_data = temp_df.iloc[1:, :].fillna('').to_dict(orient='records')



            for k , data in enumerate(temp_data):

                table_dict[dictionary_keys[0]].append(data['Unnamed: 1']) 
                table_dict[dictionary_keys[1]].append(data['Unnamed: 2'])
                table_dict[dictionary_keys[2]].append(data['Unnamed: 3']) 
                table_dict[dictionary_keys[3]].append(data['Unnamed: 4'])
                table_dict[dictionary_keys[4]].append(data['Unnamed: 5'])
                table_dict[dictionary_keys[5]].append(data['Unnamed: 6'])
                table_dict[dictionary_keys[6]].append(data['Unnamed: 7'])
                table_dict[dictionary_keys[7]].append(data['Unnamed: 8'])


        # Extract last section separately
        last_temp_df = table.iloc[indices[-1]:]
        last_temp_data = last_temp_df.iloc[1:, :].fillna('').to_dict(orient='records')

        for _, data in enumerate(last_temp_data):

            table_dict[dictionary_keys[0]].append(data['Unnamed: 1']) 
            table_dict[dictionary_keys[1]].append(data['Unnamed: 2'])
            table_dict[dictionary_keys[2]].append(data['Unnamed: 3']) 
            table_dict[dictionary_keys[3]].append(data['Unnamed: 4'])
            table_dict[dictionary_keys[4]].append(data['Unnamed: 5'])
            table_dict[dictionary_keys[5]].append(data['Unnamed: 6'])
            table_dict[dictionary_keys[6]].append(data['Unnamed: 7'])
            table_dict[dictionary_keys[7]].append(data['Unnamed: 8'])
            
        df_reformatted = pd.DataFrame(table_dict)    
        return df_reformatted

    def find_start_rows(self, df):
        # Find the start row of each table
        start_rows = df[df.iloc[:, 2] == 'Emterra Environmental (PEEL CONTRACT)'].index
        return start_rows

    def extract_table_data(self, df, start_rows):
        try:
            df_list = []
            for i, start_row in enumerate(start_rows):
                if i < len(start_rows)-1:
                    End_row = start_rows[i+1]
                    table_data = df.iloc[start_row + 1:End_row].reset_index(drop=True)
                else:
                    table_data = df.iloc[start_row + 1:].reset_index(drop=True)


                extracted_data = {'Table Number':[], 'Date':[],'ROUTE MANAGER':[]}


                # Check if the table data has "DATE" in the third column
                if 'ROUTE MANAGER' in table_data.iloc[:, 2].values:

                    # Check if 'DATE' column exists
                    if 'DATE' in table_data.iloc[:, 2].values:
                        # Extract relevant columns
                        extracted_table = table_data[table_data.iloc[:, 2] == 'DATE'].iloc[:, [2, 3, 4]]
                        extracted_table_2 = table_data[table_data.iloc[:, 2] == 'ROUTE MANAGER'].iloc[:, [2, 3]]

                        # Append extracted data to dictionary lists
                        extracted_data['Table Number'].append(f'Table_{i}')
                        extracted_data['Date'].extend(extracted_table.iloc[:, 1])
                        extracted_data['ROUTE MANAGER'].extend(extracted_table_2.iloc[:, 1])

                        ## All data in each table
                        extracted_table_3 = table_data.iloc[3:, 0:]
                        extracted_table_3.reset_index(drop=True, inplace=True)

                    else:
                        # If 'DATE' column doesn't exist, add 'NA'
                        extracted_data['Date'].extend(['NA'])
                        extracted_data['Table Number'].append(f'Table_{i}')
                        extracted_data['ROUTE MANAGER'].extend(extracted_table_2.iloc[:, 1])

                        ## All data in each table
                        extracted_table_3 = table_data.iloc[2:, 0:]
                        extracted_table_3.reset_index(drop=True, inplace=True)


                    # Filter out numerical values in Unnamed: 0
                    new_table = extracted_table_3[extracted_table_3['Unnamed: 0'].apply(lambda x: isinstance(x, str))]

                    # keeping index where data exist
                    indices = list(new_table.index)
                    

                    # Extract proper keys of dictionay for each table
                    dictionary_keys, table_detail_dict = self.each_table_keys(extracted_table_3)

                    

                    # Reset the index
                    extracted_table_3.set_index('Unnamed: 0', inplace=True)

                    df_reformatted = self.each_table_data(indices, dictionary_keys, table_detail_dict, extracted_table_3)

                    
                    
                    
                    df_reformatted_dropped = df_reformatted.replace('', pd.NA)
                    df_reformatted_dropped['Table Number'] = extracted_data['Table Number'][0]
                    df_reformatted_dropped['Date'] = extracted_data['Date'][0]
                    df_reformatted_dropped['ROUTE MANAGER'] = extracted_data['ROUTE MANAGER'][0]
                    df_reformatted_dropped =  df_reformatted_dropped[ df_reformatted_dropped['Tablet #'] != 'TOTAL']
                    df_reformatted_dropped.reset_index(drop=True, inplace=True)
                    df_list.append(df_reformatted_dropped)
            
                
            return df_list

        except Exception as e:
            logging.error(f"An error occurred while extracting table data: {e}")


    def get_first_part(self, text):
        return text.split()[0]



    def time_lineup_lookup(self, filtered_merged_df):

        driver_time_list = []
        helper_time_list = []

        for index, row in filtered_merged_df.iterrows():
        
            driver = self.df_time.loc[self.df_time['Route area'] == row['ROUTE A'], 'Driver'].iloc[0]  
            helper = self.df_time.loc[self.df_time['Route area'] == row['ROUTE A'], 'Helper'].iloc[0]

            driver_time_list.append(driver)
            helper_time_list.append(helper)
        
        filtered_merged_df['Driver Time'] = driver_time_list
        filtered_merged_df['Helper Time'] = helper_time_list
        return filtered_merged_df

    def formatting(self):
        try:
            
            start_rows1 = self.find_start_rows(self.df_lineup)
            start_rows = list(start_rows1)
            
            
            df_list = self.extract_table_data(self.df_lineup, start_rows)

            
            
            merged_df = pd.concat(df_list, ignore_index=True)
            
            # Adding date column
            wanted_time = list(merged_df['Date'].unique())[0]
            merged_df['Date'] = wanted_time
            
            merged_df['ROUTE MANAGER'] = merged_df['ROUTE MANAGER'].str.strip()
            merged_df['ROUTE A'] =  merged_df['ROUTE MANAGER'].apply(self.get_first_part)
            
            # Maybe we need to add more allowed values
            allowed_values = ['SWA', 'SWB', 'NA', 'NB', 'COMMERCIAL', 'Mix']
            filtered_merged_df = merged_df[merged_df['ROUTE A'].isin(allowed_values)]
            filtered_merged_df.dropna(subset=['TRUCK'], inplace=True)
            
            self.df_time['Route area'] =  self.df_time['Route'].apply(self.get_first_part)
            filtered_merged_df = self.time_lineup_lookup(filtered_merged_df)
            
            # Extra steps in cleaning
            # 1- Update column names:
            filtered_merged_df = filtered_merged_df.rename(columns={'NOTE': 'Collection Type', 'Details': 'Helper/Loader',
                                        'Driver Time': 'Driver Start Time', 'Helper Time': 'Helper / Loader Start Time'})
            # 2- Drop rows based on null values on "TRUCK" Column:
            filtered_merged_df.dropna(subset=['TRUCK'], inplace=True)
            
            filtered_merged_df.dropna(subset=['Collection Type', 'Driver',
                            'Helper/Loader', 'Tablet #',
                            'Second Note', 'Comments'], how='all', inplace=True)
            # 3- Drop unwanted columns:
            filtered_merged_df.drop(columns=['Table Number', 'ROUTE A'], inplace=True)
            
            # 4- Formatting Date column
            filtered_merged_df['Date'] = filtered_merged_df['Date'].dt.date
            
            # 5- Drop 'Unnamed' columns
            filtered_merged_df.drop(filtered_merged_df.columns[filtered_merged_df.columns.str.contains('Unnamed',case = False)],axis = 1, inplace = True)
            
            # 6- Extracting Driver and Helper stacking together
            driver_df = filtered_merged_df[['Type', 'TRUCK', 'Collection Type', 'Driver',
            'Tablet #', 'Second Note', 'Comments', 'Date', 'ROUTE MANAGER',
            'Driver Start Time']]
            driver_df.rename(columns={'Driver': 'Full Name', 'Driver Start Time':'Start Time'}, inplace = True)
            driver_df.loc[:, 'Driver/Helper'] = 'Driver'
            
            
            helper_df = filtered_merged_df[['Type', 'TRUCK', 'Collection Type', 'Helper/Loader',
            'Tablet #', 'Second Note', 'Comments', 'Date', 'ROUTE MANAGER', 'Helper / Loader Start Time']]
            helper_df.rename(columns={'Helper/Loader': 'Full Name', 'Helper / Loader Start Time':'Start Time'}, inplace = True)
            helper_df.dropna(subset=['Full Name'], how='all', inplace=True)
            helper_df.loc[:, 'Driver/Helper'] = 'Helper'
            
            
            final_df = pd.concat([driver_df, helper_df])
            #print(final_df)

            # There are three men truck means we have sometimes "/" in Full Name column that needs to be added as new rows
            final_df["Full Name"] = final_df["Full Name"].str.split("/") 
            final_df = final_df.explode("Full Name").reset_index(drop=True)


            final_df["Full Name"] = final_df["Full Name"].str.strip()



            # Resetting index to make sure it's continuous
            final_df.reset_index(drop=True, inplace=True)
            
            
            return final_df

        except Exception as e:
            logging.error(f"An error occurred while extracting table data: {e}")

            


    def process_xlsm_files_lineup(self):
        final_df = self.formatting()
        return final_df
    
