from time import sleep
import random
from multiprocessing import Process
import sqlalchemy as sa
from sqlalchemy import text
import yaml

class AWSDBConnector:

    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
        self.engine = self.create_db_connector()

    def create_db_connector(self):
        creds = self.read_db_creds()
        db_url = "mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4".format(
            user=creds['USER'],
            password=creds['PASSWORD'],
            host=creds['HOST'],
            port=creds['PORT'],
            database=creds['DATABASE']
        )

        engine = sa.create_engine(db_url)
        return engine

    def read_db_creds(self):
        with open(self.yaml_file, 'r') as file:
            creds = yaml.safe_load(file)
        return creds
    

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            print(geo_result)
            print(user_result)




random.seed(100)

new_connector = AWSDBConnector(yaml_file='db_creds.yaml')

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


