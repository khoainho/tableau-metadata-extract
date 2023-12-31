import os
import pandas as pd 
import tableauserverclient as TSC
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from pickle import FALSE

# Connecting to Tableau Server

def get_workbook_connection_info(user, password, site):
    tableau_auth = TSC.TableauAuth(user, password, site_id=site, user_id_to_impersonate=None)
    server = TSC.Server("https://[your_tableau_server_link_here].com", user_server_version=True)
    server.version = '3.5'
    with server.auth.sign_in(tableau_auth):
        wb_connection_info = server.metadata.query(
            """
                query wbWithCustomSQL{
                    customSQLTables{
                        name
                        id
                        query
                        downstreamWorkbooks {
                            id
                            name
                            projectName
                            updatedAt
                            owner{
                                name
                                username
                            }
                            site{
                                name
                            }
                        }
                    }
                }
            """
        )
        print('Connection to Tableau has been Established')
    return wb_connection_info

# Connection to Snowflake server
def get_snowflake_engine(user, pwd):
    account_identifier = '[Your identifier here]'
    database_name = '[Your database name here]'
    schema_name = '[Your schema name here]'
    warehouse_name = '[Your warehouse name here]'
    role_name = '[Your role name here]'
    #Connection to Snowflake
    connection_string = f"snowfalke://{user}:{pwd}@{account_identifier}/{database_name}/{schema_name}?warehouse={warehouse_name}&role={role_name}"
    engine = create_engine(connection_string)
    print("Connection to Snowflake has been Established")
    return engine

# Extracting data from downstream workbooks
def extract_downstreamWorkbooks(dataframe_af):
    dataframe_af.reset_index(inplace=True)
    dataframe_af.drop(columns=["index"], inplace=True)

    dataframe_af['donwstreamWorkbooks'] = dataframe_af['downstreamWorkbooks'].apply(remove_brackets)
    df = pd.json_normalize(dataframe_af['downsteamWorkbooks'])
    dataframe_af = pd.concat([dataframe_af, df], axis=1)
    dataframe_af = dataframe_af.rename({
        "id":"workbook_id",
        "projectName":"project_name",
        "site.name":"site_name",
        "name":"workbook_name",
        "updatedAt":"refreshed_time",
        "owner.name":"owner_name",
        "owner.username":"owner_id"
    },  
    axis=1)
    return dataframe_af

# Main function
def main():
    try:
        # Authentication
        with open(".env", "r") as file:
            auth = file.read().splitlines()
        file.close()
        username = auth[0]
        password = auth[1]
        os.environ ['HTTP_PROXY'] = "http://"+username+":"+password+"@proxy.kdc.[organization].com:0000"
        os.environ ['HTTPS_PROXY'] = "https://"+username+":"+password+"@proxy.kdc.[organization].com:0000"

        # Get workbook info 
        TSC_AF = get_workbook_connection_info(username, password, '[Your site name here]')
        raw_dataframe = pd.DataFrame(TSC_AF['data']['customSQLTables'], columns=['downstreamWorkbooks','name','id','query'])

        final_dataframe = extract_downstreamWorkbooks(raw_dataframe)

        # Publish to Snowflake
        if_exists = 'replace'
        engine = get_snowflake_engine(username, password)
        with engine.connect() as con:
            final_dataframe.to_sql(name='[Name your new Snowflake table here]', con=con, index=False, if_exists='replace')
        print("Tableau info has been ssuccessfully uploaded into Snowflake")

    except Exception as e:
        print("Error:" +str(e))
    finally:
        return
    
if __name__ == "__Main__":
    main()