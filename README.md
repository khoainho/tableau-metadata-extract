# Tableau Metadata Extract

This tool is designed to help you with the extraction of metadata from Tableau Workbooks and upload it into Snowflake.

## Technologies

* Python
* Pandas
* tableauserverclient
* pickle
* sqlalchemy
* snowflake

## Installation and Setup Instructions

 1. Run this command to install the required libraries

```python
pip install -r requirements.txt
```

 3. Create a .env file inside the Batch_Process folder and include the following info

```python
    USER = ${Place your username here}
    PWD = ${Place your passwrod here}
```

 5. Replace the environment variables in the script with your requirements

 6. Run the following command

 ```python
 python src/metadata.py
```
