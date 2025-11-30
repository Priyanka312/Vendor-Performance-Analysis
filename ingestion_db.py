import pandas as pd
import os
from sqlalchemy import create_engine
import logging 
import time

logging.basicConfig(
    filename='logs/ingestion_db.log',
    level=logging.DEBUG,
    format="%(asctime)s-%(levelname)s-%(message)s")

engine =create_engine('sqlite:///inventory.db')

#created script to ingest data in database. We are using replace instead of appending as in companies we might need to append
def ingest_db(df, table_name,engine):
    ''' This function will ingest the dataframe into database table '''
    df.to_sql(table_name,con=engine,if_exists='replace',index=False)
  
def load_raw_data():
    ''' This function will load the CSV's as dataframe and ingest into db''' 
    star=time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df=pd.read_csv('data/'+file)
            logging.info('Ingesting {file} in db')
            ingest_db(df,file[:-4],engine) #kept same file names by removing file extensions as '.csv'=4
    end=time.time()
    total_time=(end-start)/60
    logging.info('------------Ingestion completed-----------')
    logging.info(f'Total Time Taken: {total_time} minutes')

if __name__=='__main__':
    load_raw_data()