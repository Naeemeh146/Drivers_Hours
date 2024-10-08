from flask import Flask, request, jsonify
import os
from Time_Processor_Tool import TimeProcessor
from Lineup_Processor_Tool_object import LineupProcessor
from sharepointmanager import SharePointManager
from timesharepoint import TimeSharePointManager

app = Flask(__name__)

@app.route('/process_files', methods=['POST'])
def process_files():
    FOLDER_NAME = 'Python Scripts/DriversOperations_RawData'
    FOLDER_NAME_TIME = 'Python Scripts/Time_Info'
    FOLDER_NAME_DEST = 'Python Scripts/DriversOperations_UploadtoQBData'
    read = True
    
    # Initialize SharePointManager
    sharepoint_manager = SharePointManager(read, FOLDER_NAME, FOLDER_NAME_DEST)
    df_lineup, df_dow, dayname = sharepoint_manager.process_files()
    print(dayname)
    
    # Manual change on df_lineup
    row_idx = 399
    col_idx = 3
    specific_value = df_lineup.iat[row_idx, col_idx]
    df_lineup.iat[row_idx, col_idx] = f"Mix {specific_value}"

    # Initialize TimeProcessor and process files
    timesharepoint = TimeSharePointManager(read, dayname ,FOLDER_NAME_TIME)
    df_time = timesharepoint.process_files()
    print(df_time)

    # Initialize LineupProcessor and process files
    processor = LineupProcessor(df_lineup, df_time)
    final_df = processor.process_xlsm_files_lineup()
    
    # Final processing with SharePointManager
    sharepoint_manager_upload = SharePointManager(False, FOLDER_NAME, FOLDER_NAME_DEST, final_df)
    sharepoint_manager_upload.process_files()

    return jsonify({"message": "Files processed successfully"}), 200

if __name__ == "__main__":
    app.run(debug=True)