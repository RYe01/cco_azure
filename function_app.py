import azure.functions as func
import logging
import pandas as pd
import io
import hashlib
import pyodbc
import os

from adlfs import AzureBlobFileSystem

# Initialize AzureBlobFileSystem with your Azure storage account details
account_name = os.environ.get('STORAGE_ACCOUNT_NAME', 'default_account_name')
account_key = ('STORAGE_ACCOUNT_KEY', 'default_account_key')
database_password = os.environ.get('DATABASE_PASSWORD', 'default_password')

file_system = AzureBlobFileSystem(account_name=account_name, account_key=account_key)

# Setup your database connection string and encryption key
DATABASE_URL = f"jdbc:sqlserver://cco-anonym.database.windows.net:1433;database=cco;user=CloudSA87b4a913@cco-anonym;password={database_password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# Establish a database connection
cnxn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};Server=tcp:cco-anonym.database.windows.net,1433;Database=cco;Uid=CloudSA87b4a913;Pwd=Jsdngasas123;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
cursor = cnxn.cursor()

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="cco-anonym/arriving_files/{name}.csv",
                               connection="BlobStorageConnectionString") 
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

    df = pd.read_csv(io.BytesIO(myblob.read()))

    def check_and_create_table(cursor):
        create_table_query = """
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'name_mapping')
        BEGIN
            CREATE TABLE name_mapping (
                original_name NVARCHAR(MAX),
                hashed_name NVARCHAR(MAX)
            );
        END
        """
        cursor.execute(create_table_query)
        cursor.commit()

    # Define the function to generalize the 'age' column
    def generalize_age(age):
        if age < 18:
            return 'child'
        elif age < 60:
            return 'adult'
        else:
            return 'senior'
        
    # Define the function to hash the 'name' column
    def pseudonymize_names(names):
        # Hash names and save the mapping
        name_mapping = names.apply(lambda x: (x, hashlib.sha256(str(x).encode()).hexdigest()) if pd.notnull(x) else (x, 'Name Unknown'))
        for original_name, hashed_name in name_mapping:
            cursor.execute("INSERT INTO name_mapping (original_name, hashed_name) VALUES (?, ?)", original_name, hashed_name)
        cnxn.commit()
        return name_mapping.apply(lambda x: x[1])
    
    def generalize_fare(fare):
        if fare < 10:
            return 'low'
        elif fare < 50:
            return 'medium'
        elif fare < 100:
            return 'high'
        else:
            return 'very high'
    
    # number of siblings
    def generalize_sibsp(sibsp):
        if sibsp == 0:
            return '0'
        elif sibsp <= 2:
            return '1-2'
        else:
            return '3+'
        
    check_and_create_table(cursor)
        
    df['name'] = df['name'].fillna('Unknown')
        
    # Apply anonymization (generalize)
    df['age'] = df['age'].apply(generalize_age)
    df['name'] = pseudonymize_names(df['name'])
    df['fare'] = df['fare'].apply(generalize_fare)
    df['sibsp'] = df['sibsp'].apply(generalize_sibsp)
    df['parch'] = df['parch'].apply(generalize_sibsp)

    # Drop or anonymize other sensitive columns
    df.drop(columns=['ticket', 'cabin', 'body'], inplace=True)

    print(df['name'])

    # Check for k-anonymity (for k=2 as an example)
    k = 2
    quasi_identifiers = ['sex', 'age', 'pclass', 'sibsp', 'parch', 'fare', 'embarked']
    counts = df.groupby(quasi_identifiers).size()

    # Filter out groups that don't meet k-anonymity
    non_anonymous = counts[counts < k].reset_index()[quasi_identifiers]
    non_anonymous = pd.merge(non_anonymous, df, on=quasi_identifiers, how='left')

    df_anonymous = df.drop(non_anonymous.index)

    # Now df_anonymous should be k-anonymous with respect to 'age' and 'sex'

    print("Failed:")
    print(non_anonymous)

    # Define the path where the anonymized CSV will be stored
    anonymized_blob_path = f"cco-anonym/anonym/{myblob.name.split('/')[-1]}"

    # Convert the DataFrame to a CSV string
    csv_data = df_anonymous.to_csv(index=False, lineterminator="\n")

    # Save the anonymized CSV back into Azure Blob Storage
    with file_system.open(anonymized_blob_path, 'w', newline='\n') as f:
        f.write(csv_data)

    logging.info(f"Anonymized data saved to blob: {anonymized_blob_path}")

    # Delete the original blob from 'arriving_files' after processing
    arriving_files_path = f"cco-anonym/arriving_files/{myblob.name.split('/')[-1]}"
    try:
        file_system.rm(arriving_files_path)
        logging.info(f"Original blob deleted: {arriving_files_path}")
    except Exception as e:
        logging.error(f"Failed to delete original blob: {arriving_files_path}. Error: {str(e)}")