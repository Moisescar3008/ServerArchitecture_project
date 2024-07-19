import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import time
from datetime import datetime, timedelta

# Initialize global variables for continuity
start_date = datetime.strptime('2023-05-16', '%Y-%m-%d')
register_date = datetime.strptime('2023-08-18', '%Y-%m-%d')
counter = 1
db_table = 'farm' #Set the name of the table


# Generate Random Data
def generate_random_data():
    global start_date, register_date, counter, db_table
    
    rows = []
    n = 5
    for _ in range(n):  # Generate n rows each time
        
        # Add a random number of days (between 1 and 8) to the previous record's date
        increment_days = np.random.randint(1, 9)
        record_date = start_date + timedelta(days=increment_days)
        id_str = f"{record_date.strftime('%Y-%m-%d')}-{counter:06d}"
        row = {
            'id': id_str,
            'counter': counter,
            'fecha_registro': register_date.strftime('%Y-%m-%d'),
            'fecha_nacimiento': record_date.strftime('%Y-%m-%d'),
            'tipo': np.random.choice(['R', 'S']),
            'volumen': np.random.randint(40, 101)
        }
        rows.append(row)
        counter += 1
        start_date = record_date  # Update start_date to the new record date

    print(f'_____Row {counter-n} to {counter-1} done.')
    
    return pd.DataFrame(rows)

# Database Connection
def send_dataframe_to_database(df):
    global db_table

    db_username = 'root'
    db_password = 'password'  # Insert your password
    db_host = '127.0.0.1'  # Insert your host
    db_port = '3306'  # Insert your port
    db_name = 'db1' #Insert name of the DB 
    connection_str = f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    
    engine = create_engine(connection_str)
    conn = engine.connect()
    
    # Insert Data into Database
    df.to_sql(name=db_table, con=conn, if_exists='append', index=False)
    
    # Close Connection
    conn.close()

# Check table and get the latest fecha_registro and counter
def check_table_and_set_globals():
    global start_date, register_date, counter, db_table
    
    db_username = 'root'
    db_password = 'password'  # Insert your password
    db_host = '127.0.0.1'  # Insert your host
    db_port = '3306'  # Insert your port
    db_name = 'db1' #Insert name of the DB 
    connection_str = f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    
    engine = create_engine(connection_str)
    conn = engine.connect()
    
    # Create the table if it does not exist
    conn.execute(text(f"""
    CREATE TABLE IF NOT EXISTS {db_table} (
        id VARCHAR(50) PRIMARY KEY,
        counter INT,
        fecha_registro DATE,
        fecha_nacimiento DATE,
        tipo CHAR(1),
        volumen INT
    )
    """))
    
    # Check if the table contains data
    result = conn.execute(text(f"SELECT MAX(fecha_registro) AS max_fecha_registro, MAX(counter) AS max_counter, MAX(fecha_nacimiento) AS max_fecha_nacimiento FROM {db_table}"))
    row = result.fetchone()
    
    if row and row[0]:
        register_date = pd.to_datetime(row[0])
        counter = row[1] + 1 if row[1] else 1
        start_date = pd.to_datetime(row[2]) if row[2] else register_date - timedelta(days=1)
        print(f"Table already contains data. Continuing from counter: {counter} / fecha_registro: {register_date} / fecha_nacimiento: {start_date}")
    else:
        print("Table is empty. Starting from initial values.")
    
    conn.close()

# Main Execution
if __name__ == "__main__":
    check_table_and_set_globals()
    
    n = 2
    for counterr in range(n):  # Loop
        # Create DataFrame
        df = generate_random_data()  # Generate rows of random data
        
        # Send DataFrame to Database
        send_dataframe_to_database(df)
        print(f'**** Injection #{counterr+1} complete!****')
        
        # Wait for x seconds
        time.sleep(2)

