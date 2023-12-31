# Tableau Metadata Extraction Tool
This tool is designed to help you extract the metadata in every Tableau Workbook in your organization and upload it into Snowflake

## Installation and Setup Instructions
 
 1. Run this command to install the required libraries 
    ```pip install -r requirements.txt```
 2. Create a .env file inside the Batch_Process folder and include the following info
    * USER={your username}
    * PWD={your password}
 3. Replace the enviorment variables in the script to your requirements
 4. Run the following command
    ```metadata.py```