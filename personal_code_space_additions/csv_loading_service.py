#libraries
import os
import psycopg2
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential

# IMPORTANT! This code is for demonstration purposes only. It's not suitable for use in production. 
# For example, tokens issued by Microsoft Entra ID have a limited lifetime (24 hours by default). 
# In production code, you need to implement a token refresh policy.

def get_connection_uri():

    # Read URI parameters from the environment
    dbhost = os.getenv("PGHOST")
    dbname = os.getenv("PGDATABASE")
    dbuser = os.getenv("PGUSER")
    sslmode = os.getenv("PGSSLMODE")

    # Use passwordless authentication via DefaultAzureCredential.
    # IMPORTANT! This code is for demonstration purposes only. DefaultAzureCredential() is invoked on every call.
    # In practice, it's better to persist the credential across calls and reuse it so you can take advantage of token
    # caching and minimize round trips to the identity provider. To learn more, see:
    # https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/identity/azure-identity/TOKEN_CACHING.md 
    #credential = DefaultAzureCredential()

    # Call get_token() to get a token from Microsft Entra ID and add it as the password in the URI.
    # Note the requested scope parameter in the call to get_token, "https://ossrdbms-aad.database.windows.net/.default".
    #password = credential.get_token("https://ossrdbms-aad.database.windows.net/.default").token

    #db_uri = f"postgresql://{dbuser}:{password}@{dbhost}/{dbname}?sslmode={sslmode}"
    #return db_uri

def main():
    #load in env vars
    load_dotenv()

    #open file
    with open("Bank_Customer_Churn_Prediction.csv", "r") as churn_file:
        #upload list of lists
        churn_csv_upload_source = list()
        #convert churn data to lines
        churn_lines = churn_file.readlines()
        #retrieve header
        header = churn_lines[0:1]
        #access header rows
        header_columns = header[0].split(",")
        #skip header
        remaining_churn_lines = churn_lines[1::]
        #process data into upload source list
        for line in remaining_churn_lines:
            churn_row_items = line.split(",")
            churn_row_items = [item.strip("\n") for item in churn_row_items]
            churn_csv_upload_source.append(churn_row_items)
        
        #create context
        #conn = psycopg2.connect(os.getenv(""))
        usr = os.getenv("PGUSER")
        pswd = os.getenv("PGPASSWORD")#.replace("@", "%40")
        hst = os.getenv("PGHOST")
        prt = int(os.getenv("PGPORT"))
        dtb = os.getenv("PGDATABASE")
        ssl = os.getenv("PGSSLMODE")
        az_conn_str = os.getenv("AZURE_POSTGRES_SQL_CONNECTION_STRING")
        #print(f"user={usr}, password={pswd}, host={hst}, port={prt}, dbname={dtb} ssl{ssl}")
        #conn_string = f"postgresql://{hst}:{prt}/{dtb}?user={usr}&password={pswd}&sslmode=require"
        conn_string = f"host={hst} dbname={dtb} user={usr} password=\"{pswd}\" sslmode={ssl}"
        #print(conn_string)
        #conn_string = get_connection_uri()
        context = psycopg2.connect(az_conn_str)
        cursor = context.cursor()
        
        for source in churn_csv_upload_source:
            #SQL = """INSERT INTO vendors(vendor_name)
             #VALUES(%s) RETURNING vendor_id;"""
            cursor.execute("INSERT INTO retention_history (customer_id, credit_score, country, gender, age, tenure, balance, products_number, credit_card, active_member, estimated_salary, churn) VALUES (%s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s, %s);", source)

        context.commit()
        cursor.close()
        print("end of process")

if __name__ == "__main__":
    main()