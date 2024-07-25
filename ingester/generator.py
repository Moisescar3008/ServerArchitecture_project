import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import time

class FarmDataGenerator:
    def __init__(self, start_date_str, num_iterations=2, wait_time=2, num_rows=5, initial_counter=1):
        self.start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        self.num_iterations = num_iterations
        self.wait_time = wait_time
        self.num_rows = num_rows
        self.counter = initial_counter

    def generate_random_data(self):
        rows = []
        for _ in range(self.num_rows):
            # Generate random month and day within the specified range
            random_month = np.random.randint(5, 11)  # May (5) to October (10)
            random_day = np.random.randint(1, 29)  # To ensure no invalid dates
            record_date = self.start_date.replace(month=random_month, day=random_day)
            
            # Calculate the register date
            if random_month == 5:
                register_date = record_date.replace(month=4, day=15)  # April 15
            else:
                register_date = record_date.replace(month=random_month-1, day=15)  # Previous month 15th

            row = {
                'fecha_registro': register_date.strftime('%Y-%m-%d'),
                'fecha_nacimiento': record_date.strftime('%Y-%m-%d'),
                'tipo': np.random.choice(['R', 'S']),
                'volumen': np.random.randint(40, 101)
            }
            rows.append(row)
            self.counter += 1

        print(f'_____Row {self.counter-self.num_rows} to {self.counter-1} done.')
        return pd.DataFrame(rows)

    def save_dataframe_to_json(self, df, filename="farm_data.json"):
        with open(filename, 'w') as file:
            json.dump(df.to_dict(orient='records'), file, indent=4)

    def run(self):
        for i in range(self.num_iterations):
            df = self.generate_random_data()
            self.save_dataframe_to_json(df, filename=f"farm_data_{i+1}.json")
            print(f'**** Injection #{i+1} complete!\n ****')
            time.sleep(self.wait_time)

if __name__ == "__main__":
    generator = FarmDataGenerator(start_date_str='2023-05-16', num_iterations=10, wait_time=5, num_rows=10)
    generator.run()
