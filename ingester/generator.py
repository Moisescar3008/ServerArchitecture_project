import numpy as np
import json
from datetime import datetime
import time

class FarmDataGenerator:
    def __init__(self, start_date_str):
        self.start_date = datetime.strptime(start_date_str, '%Y-%m-%d')

    def generate_random_data(self):
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
        
        return row
    
    # Bucle to generate random data on a JSON format.
    def run(self, wait_time=5, num_jsons=10):
        while True:
            json_data_list = [self.generate_random_data() for _ in range(num_jsons)]
            for data in json_data_list:
                print(json.dumps(data, indent=4))
            time.sleep(wait_time)

if __name__ == "__main__":
    generator = FarmDataGenerator(start_date_str='2023-05-16')
    generator.run(wait_time=2, num_jsons=1)  # Adjust wait_time and num_jsons as needed
