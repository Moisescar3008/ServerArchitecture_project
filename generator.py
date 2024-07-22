import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import time
from datetime import datetime, timedelta, date as dt_date

# Initialize global variables for continuity
start_date = datetime.strptime('2023-05-16', '%Y-%m-%d')
register_date = datetime.strptime('2023-08-18', '%Y-%m-%d')
counter = 1

# Generate Random Data
def generate_random_data():
    global start_date, register_date, counter
    
    rows = []
    n = 5
    for _ in range(n):  # Generate 5 rows each time
        
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

    print(f'_____Row {counter-5} to {counter} done.')
    
    return pd.DataFrame(rows)

# Database Connection
def send_dataframe_to_database(df):
    db_username = 'postgres'
    db_password = 'password'  # Insert your password
    db_host = '127.0.0.1'  # Insert your host
    db_port = '5432'  # Default PostgreSQL port
    db_name = 'db1'
    connection_str = f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    
    engine = create_engine(connection_str)
    conn = engine.connect()
    
    # Insert Data into Database
    df.to_sql(name='farm', con=conn, if_exists='append', index=False)
    
    # Close Connection
    conn.close()

# Check table and get the latest fecha_registro and counter
def check_table_and_set_globals():
    global start_date, register_date, counter
    
    db_username = 'postgres'
    db_password = 'password'  # Insert your password
    db_host = '127.0.0.1'  # Insert your host
    db_port = '5432'  # Default PostgreSQL port
    db_name = 'db1'
    connection_str = f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    
    engine = create_engine(connection_str)
    conn = engine.connect()
    
    # Create the table if it does not exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS farm (
        id VARCHAR PRIMARY KEY,
        counter INTEGER,
        fecha_registro DATE,
        fecha_nacimiento DATE,
        tipo CHAR(1),
        volumen INTEGER
    );
    """
    conn.execute(text(create_table_sql))

    # Check if the table contains data
    result = conn.execute(text("SELECT MAX(fecha_registro) AS max_fecha_registro, MAX(counter) AS max_counter, MAX(fecha_nacimiento) AS max_fecha_nacimiento FROM farm"))
    row = result.fetchone()
    
    if row and row[0]:
        register_date = row[0] if isinstance(row[0], (datetime, dt_date)) else datetime.strptime(str(row[0]), '%Y-%m-%d')
        counter = row[1] + 1 if row[1] else 1
        start_date = row[2] if isinstance(row[2], (datetime, dt_date)) else datetime.strptime(str(row[2]), '%Y-%m-%d') if row[2] else register_date - timedelta(days=1)
        print(f"Table already contains data. Continuing from counter: {counter} / fecha_registro: {register_date} / fecha_nacimiento: {start_date}")
    else:
        print("Table is empty. Starting from initial values.")
    
    conn.close()
    

# Main Execution
if __name__ == "__main__":
    check_table_and_set_globals()
    
    for counterr in range(2):  # Loop
        # Create DataFrame
        df = generate_random_data()  # Generate rows of random data
        
        # Send DataFrame to Database
        send_dataframe_to_database(df)
        print(f'**** Injection #{counterr+1} complete!\n ****')
        
        # Wait for x seconds
        time.sleep(2)
