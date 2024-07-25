import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import time
from datetime import datetime, timedelta, date as dt_date
import psycopg2

class FarmDataGenerator:
    def __init__(self, db_config, start_date_str, register_date_str, initial_counter=1):
        self.db_username = db_config['username']
        self.db_password = db_config['password']
        self.db_host = db_config['host']
        self.db_port = db_config['port']
        self.db_name = db_config['dbname']
        self.start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        self.register_date = datetime.strptime(register_date_str, '%Y-%m-%d')
        self.counter = initial_counter

        self.check_and_create_database()
        self.engine = create_engine(self.get_connection_str())

    def get_connection_str(self, dbname=None):
        dbname = dbname if dbname else self.db_name
        return f'postgresql+psycopg2://{self.db_username}:{self.db_password}@{self.db_host}:{self.db_port}/{dbname}'

    def check_and_create_database(self):
        connection_str = f"dbname={self.db_name} user='{self.db_username}' host='{self.db_host}' password='{self.db_password}' port='{self.db_port}'"
        conn = psycopg2.connect(connection_str)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.db_name}'")
        exists = cur.fetchone()
        if not exists:
            cur.execute(f"CREATE DATABASE {self.db_name}")
            print(f"Database {self.db_name} created.")
        else:
            print(f"Database {self.db_name} already exists.")

        cur.close()
        conn.close()

    def generate_random_data(self, num_rows=5):
        rows = []
        for _ in range(num_rows):
            increment_days = np.random.randint(1, 9)
            record_date = self.start_date + timedelta(days=increment_days)
            id_str = f"{record_date.strftime('%Y-%m-%d')}-{self.counter:06d}"
            row = {
                'id': id_str,
                'counter': self.counter,
                'fecha_registro': self.register_date.strftime('%Y-%m-%d'),
                'fecha_nacimiento': record_date.strftime('%Y-%m-%d'),
                'tipo': np.random.choice(['R', 'S']),
                'volumen': np.random.randint(40, 101)
            }
            rows.append(row)
            self.counter += 1
            self.start_date = record_date  # Update start_date to the new record date

        print(f'_____Row {self.counter-num_rows} to {self.counter-1} done.')
        return pd.DataFrame(rows)

    def send_dataframe_to_database(self, df):
        with self.engine.connect() as conn:
            df.to_sql(name='farm', con=conn, if_exists='append', index=False)

    def check_table_and_set_globals(self):
        with self.engine.connect() as conn:
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

            result = conn.execute(text("SELECT MAX(fecha_registro) AS max_fecha_registro, MAX(counter) AS max_counter, MAX(fecha_nacimiento) AS max_fecha_nacimiento FROM farm"))
            row = result.fetchone()

            if row and row[0]:
                self.register_date = row[0] if isinstance(row[0], (datetime, dt_date)) else datetime.strptime(str(row[0]), '%Y-%m-%d')
                self.counter = row[1] + 1 if row[1] else 1
                self.start_date = row[2] if isinstance(row[2], (datetime, dt_date)) else datetime.strptime(str(row[2]), '%Y-%m-%d') if row[2] else self.register_date - timedelta(days=1)
                print(f"Table already contains data. Continuing from counter: {self.counter} / fecha_registro: {self.register_date} / fecha_nacimiento: {self.start_date}")
            else:
                print("Table is empty. Starting from initial values.")

    def run(self, num_iterations=2, wait_time=2):
        self.check_table_and_set_globals()
        for i in range(num_iterations):
            df = self.generate_random_data()
            self.send_dataframe_to_database(df)
            print(f'**** Injection #{i+1} complete!\n ****')
            time.sleep(wait_time)

if __name__ == "__main__":
    db_config = {
        'username': 'postgres',  # Insert username
        'password': 'password',  # Insert password
        'host': '127.0.0.1',  # Insert host
        'port': '5432',  # Default PostgreSQL port [You can change it]
        'dbname': 'db1'  # Insert name of Data Base
    }
    
    generator = FarmDataGenerator(db_config, start_date_str='2023-05-16', register_date_str='2023-08-18')
    generator.run()
