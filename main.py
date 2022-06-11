"""Function called by PubSub trigger to execute cron job tasks."""
import datetime
import logging
from string import Template
import config
from google.cloud import bigquery
import requests
import json
import pandas as pd
import urllib.request
import pandas_gbq
import farmhash
import numpy as np
import gc
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
import time

pd.options.mode.chained_assignment = None
thread_local = threading.local()

##generates hash
def fingerHash(x):
    xs = [str(v) for v in x]
    hash_string = ''.join(xs)
    hash = np.uint64((farmhash.fingerprint64(hash_string))).astype('int64')
    return hash

## download file function
def downloadFile(url,session):

    """"Function downloads file from url returns dataframe """
    url_1 = url.replace('.xml','.json')

    # with urllib.request.urlopen(url_1) as url:
    #     data = json.loads(url.read())
    with session.get(url_1) as response:
        a = response.content
        data = json.loads(a)

    header = data['FHRSEstablishment']['Header']
    est = data['FHRSEstablishment']['EstablishmentCollection']
    restaurants = pd.DataFrame(est)
    restaurants['ExtractDate'] = header['ExtractDate']
    return restaurants
#
## download file function
def downloadFileThread(url):

    """"Function downloads file from url returns dataframe """
    session = get_session()
    url_1 = url.replace('.xml','.json')

    # with urllib.request.urlopen(url_1) as url:
    #     data = json.loads(url.read())
    with session.get(url_1) as response:
        a = response.content
        data = json.loads(a)

    header = data['FHRSEstablishment']['Header']
    est = data['FHRSEstablishment']['EstablishmentCollection']
    restaurants = pd.DataFrame(est)
    restaurants['ExtractDate'] = header['ExtractDate']
    return restaurants




def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def download_all_sites(sites):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        list = executor.map(downloadFileThread, sites)
    return list


###extract data from api
def ExtractRatingsData():

    url = "https://api.ratings.food.gov.uk/authorities/"

    payload={}
    headers = {
      'x-api-version': '2',
      'content-type': 'application/json',
      'Cookie': 'ApplicationGatewayAffinity=4d1236eab22e0a0044056c2fc64a1f13'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    authorities = response.text
    authority = pd.DataFrame(json.loads(authorities)['authorities']) ###REMOVE

    urls = authority['FileName'].to_list()

    # start_time = time.time()
    # df_list = []
    # with requests.Session() as session:
    #     for url in urls:
    #         df_list.append(downloadFile(url,session))
    #
    # duration = time.time() - start_time
    # print(f"Sequntial Approach took {len(urls)} in {duration} seconds")



    ### THREADING APPROACH FOR QUICKER RUN TIME #########

    start_time = time.time()
    df_futures = download_all_sites(urls)
    df_list = [df for df in df_futures]
    duration = time.time() - start_time
    print(f"Threaded approach took  {len(urls)} in {duration} seconds")





    ##concat dataframes and reset indexes
    ratings = pd.concat(df_list)
    ratings.reset_index(inplace=True,drop=True)
    ##explode key oclumns
    scores = pd.json_normalize(ratings['Scores'])
    geocode = pd.json_normalize(ratings['Geocode']).reset_index(drop=True)
    rate_1 = pd.concat([ratings,scores],axis=1)
    rates = pd.concat([rate_1,geocode],axis=1)
    rates['hashkey'] = rates[['FHRSID','RatingDate','RatingKey','RatingValue']].apply(lambda x: fingerHash(tuple(x)), axis=1)


    ##select restaurants
    restaurants = rates[['FHRSID','LocalAuthorityBusinessID','BusinessName','BusinessType','AddressLine1','AddressLine2',
    'AddressLine3','AddressLine4','PostCode','LocalAuthorityCode','LocalAuthorityName','LocalAuthorityWebSite','Latitude','Longitude','LocalAuthorityEmailAddress']]

    ##select ratings
    ratings_table = rates[['ExtractDate','FHRSID','hashkey','RatingDate','RatingKey','RatingValue','SchemeType','NewRatingPending','RightToReply','Hygiene','Structural','ConfidenceInManagement']]
    ratings_table['Hashkey'] = ratings_table['hashkey']
    ratings_table.drop('hashkey', axis=1, inplace=True)
    import gc


    del(rate_1,geocode,scores,rates,ratings)
    gc.collect()

    return restaurants,ratings_table


def file_to_string(sql_path):
    """Converts a SQL file holding a SQL query to a string.
    Args:
        sql_path: String containing a file path
    Returns:
        String representation of a file's contents
    """
    with open(sql_path, 'r') as sql_file:
        return sql_file.read()


def execute_query(bq_client):
    """Executes transformation query to a new destination table.
    Args:
        bq_client: Object representing a reference to a BigQuery Client
    """
    dataset_ref = bq_client.get_dataset(bigquery.DatasetReference(
        project=config.config_vars['project_id'],
        dataset_id=config.config_vars['output_dataset_id']))
    table_ref = dataset_ref.table(config.config_vars['output_table_name'])
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition().WRITE_APPEND
    sql = file_to_string(config.config_vars['sql_file_path'])
    logging.info('Attempting query on all dates...')
    # Execute Query
    query_job = bq_client.query(
        sql,
        job_config=job_config)

    query_job.result()  # Waits for the query to finish
    logging.info('Query complete. The table is updated.')


def executeAPI_query(bq_client):
    """Executes APi transformation query to a new destination table.
    Args:
        bq_client: Object representing a reference to a BigQuery Client
    """

    #load ratings data
    restaurants,ratings = ExtractRatingsData()
    ratings.rename(columns={'FHRSID':'fhrsid'},inplace=True)
    restaurants.rename(columns={'FHRSID':'fhrsid'},inplace=True)

    logging.info('Finished Downloading')
    #### SQL SECTION #########
    """SQL COMMANDS USED TO PULL OUT Hashkey AND FHRSID for update procedure"""
    sql = """
    SELECT
    FARM_FINGERPRINT(concat(fhrsid,RatingDate,RatingKey,RatingValue)) as Hashkey,

    FROM `ukfoodrating-1616225759827.rating.ratings`
    WHERE RAND() < 2 """


    sql_rest = """SELECT fhrsid from `ukfoodrating-1616225759827.rating.restaurants` """

    ################

    ###UPDATE PROCECURE
    #Get Hashkey, and check Hash doesnt exist in table if it does filter it out
    ratings_hash = pandas_gbq.read_gbq(sql, dialect='standard')
    restaurants_hash = pandas_gbq.read_gbq(sql_rest, dialect='standard')

    ##filter for new updates and real updates
    updates_ratings = ratings[~ratings.Hashkey.isin(ratings_hash.Hashkey.to_list())]
    new_res_updates = updates_ratings[~updates_ratings.fhrsid.isin(restaurants_hash.fhrsid.to_list())]
    real_updates = updates_ratings[~updates_ratings.RatingDate.isnull()]
    update_ratings_complete = pd.concat([new_res_updates,real_updates])



    del(ratings,ratings_hash)
    gc.collect()

    ##update restaurants
    updates_restaurants = restaurants[~restaurants.fhrsid.isin(restaurants_hash.fhrsid.to_list())]


    del(restaurants,restaurants_hash)
    gc.collect()


    ##upload
    table_id = 'rating.ratings'
    pandas_gbq.to_gbq(update_ratings_complete,table_id,project_id='ukfoodrating-1616225759827', if_exists='append')
    pandas_gbq.to_gbq(updates_restaurants,'rating.restaurants',project_id='ukfoodrating-1616225759827', if_exists='append')

    logging.info(f'Updated {len(update_ratings_complete)} to Ratings Table')
    logging.info(f'Updated {len(updates_restaurants)} to Restaurants Table')
    logging.info('Query complete. The table is updated.')




def main(data, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    bq_client = bigquery.Client()


    try:
        current_time = datetime.datetime.utcnow()
        log_message = Template('Cloud Function was triggered on $time')
        logging.info(log_message.safe_substitute(time=current_time))

        try:
            #execute_query(bq_client)

            executeAPI_query(bq_client)


        except Exception as error:
            log_message = Template('Query failed due to '
                                   '$message.')
            logging.error(log_message.safe_substitute(message=error))

    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)

if __name__ == '__main__':

    main('data', 'context')


    #gcp deploy command gcloud functions deploy RatingsDaily --entry-point main --runtime python37 --trigger-resource Testing --memory 4096MB --trigger-event google.pubsub.topic.publish --timeout 540s
