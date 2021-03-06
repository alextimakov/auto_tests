import sys
import os
sys.path.append(os.path.abspath('.'))

import auto_tests.config as config
import pandas as pd
import requests.cookies
import json
from keycloak import KeycloakOpenID
from time import sleep, time
from datetime import datetime
from pandas import json_normalize
from bson.errors import InvalidDocument
from pymongo.errors import WriteError
from argparse import ArgumentParser
import getpass


def aggregate_dash(db, collection, pipeline):
    """
    Run aggregate query on selected collection from MongoDB and return it as Pandas DataFrame.

    :param (pymongo.database.Database) db: DB name
    :param (str) collection: Collection name
    :param (list) pipeline: each pipeline stage is a dict in format {"$method": "query"}
    :return (pandas.core.frame.DataFrame): returns DataFrame with queried data from selected collection
    """

    cursor = db[collection].aggregate(pipeline)
    result = pd.DataFrame(list(cursor))
    return result


def aggregate_var(db, collection, pipeline):
    """
    Run aggregate query on selected collection from MongoDB and return it as Pandas DataFrame.

    :param (pymongo.database.Database) db: DB name
    :param (str) collection: Collection name
    :param (list) pipeline: each pipeline stage is a dict in format {"$method": "query"}
    :return (dict): returns dict with {'variables: []}
    """

    cursor = db[collection].aggregate(pipeline)
    try:
        return list(cursor)[0]
    except IndexError:
        return {'variables': []}


def mongo_request(db, collection, pipeline, *fields):
    """
    Make custom request with Mongo Aggregation Pipeline

    :param db: DB name
    :param collection: Collection name
    :param pipeline: Mongo Aggregation Pipeline
    :param fields: fields to be used as subset for drop_duplicates
    :return: return DataFrame with queried data from selected collection
    """
    result = aggregate_dash(db, collection, pipeline)
    for field in fields:
        result = result.drop_duplicates(field).reset_index(drop=True)
    for column in result.columns:
        result[column] = result[column].astype(str)
    return result


def read_mongo(db, collection, query=None, output=None, no_id=False):
    """
    Read selected collection from MongoDB and return it as Pandas DataFrame.

    :param (pymongo.database.Database) db: DB name
    :param (str) collection: Collection name
    :param (dict) query: Query to documents in selected collection, like in db.collection.find(query)
    :param (dict) output: Select needed fields of selected documents
    :param (boolean) no_id: ObjectId if True else delete ObjectId
    :return (pandas.core.frame.DataFrame): returns DataFrame with queried data from selected collection
    """

    if output is None:
        output = {}
    if query is None:
        query = {}
    cursor = db[collection].find(query, output)  # Make a query to the specific DB and Collection
    df = pd.DataFrame(list(cursor))  # Expand the cursor and construct the DataFrame
    if no_id:
        del df['_id']
    return df


def write_results(final, i, dashboard, metrics_id, text, timer, status_code, website, prefix):
    result = pd.DataFrame(
        data=[[dashboard, metrics_id, text, timer, website+str(dashboard)+'/'+str(metrics_id), status_code]],
        index=[i],
        columns=['dashboard_id', 'metric_id', 'answer_{}'.format(prefix), 'timer_{}'.format(prefix),
            'metric_{}_link'.format(prefix), 'code_{}'.format(prefix)])
    final = final.append(result)
    return final


def read_poll_job(poll_job):
    """
    Read poll_job results via json.loads

    :param (requests.session.get) poll_job:
    :return (str) text: answer from metric
    """

    try:
        text = poll_job.json()['data']
    except ValueError:
        text = poll_job.text
    return str(text)


def collect_data(db, collection_dashboards, collection_collections, pipeline, black_list):
    # Сбор списка дашбордов и метрик с MongoDB_prod
    df_prod = mongo_request(db, collection_dashboards, pipeline, 'metric_id')

    # Собираем все модули
    cursor_collections = read_mongo(db, collection_collections,
                                              query={"deleted": False}, output={'name.ru': 1})
    cursor_collections['name'] = json_normalize(cursor_collections['name'])
    collections = cursor_collections.loc[~cursor_collections['name'].isin(black_list)]['_id'].tolist()

    # собираем _id релевантных дашбордов, имя - для доппроверки
    needs_dashboards = read_mongo(db, collection_dashboards, query={
        "$and": [{"deleted": False}, {"general.published": True}, {"general.collectionId": {"$in": collections}}]},
                                            output={'_id': 1, 'general.name.ru': 1})
    # fix this later
    needs_dashboards['_id'] = needs_dashboards['_id'].astype(str)

    return df_prod.loc[df_prod['dashboard_id'].isin(needs_dashboards['_id'].unique())].reset_index(drop=True)


def run_sso_session(sso, user, password, logger, add_cookies=True, custom_headers=False, headers=None):
    if headers is None:
        headers = {}
    session = requests.Session()
    if custom_headers:
        r = session.post(sso, data={"login": user, "password": password}, timeout=5, headers=headers)
    else:
        r = session.post(sso, data={"login": user, "password": password}, timeout=5)
    logger.info("session started with code {} by {}".format(int(r.status_code), user))
    if add_cookies:
        session.cookies.set_cookie(requests.cookies.create_cookie('roles', 'admin'))
        session.cookies.set_cookie(requests.cookies.create_cookie('_currentUser', user))
    return session


def collect_parameters(i, dashboard_id, metric_id, logger, user, db_var, dashboards, var_type):
    # add check for defaultValueAsQuery
    # check for defaultQuery type and run script depending on type
    if var_type == 'default':
        # сбор тела запроса метрики из дефолтных настроек метрики
        logger.info("{} metric {} initiated by {} with {} vars".format(i, metric_id, user, var_type))
        pipeline_var_default = [{"$match": {"deleted": {"$ne": True}}},
            {"$group": {"_id": "$metrics.{}.variables".format(metric_id)}}, {"$match": {"_id": {"$ne": None}}},
            {"$project": {"_id": 0, "variables": "$_id"}}]
        aggregation = aggregate_var(db_var, dashboards, pipeline_var_default)

    elif var_type == 'logs':
        # сбор тела запроса метрики из логов
        logger.info("{} metric {} initiated by {} with {} vars".format(i, metric_id, user, var_type))

        aggregation = db_var['analytics-logs']['jobs'].find({
            "$and": [{'meta.parameters.metricData.dashboardId': "{}".format(dashboard_id)},
                     {'meta.parameters.metricData.metricId': "{}".format(metric_id)}, {'meta.state': "received"},
                     {'meta.type': "metricData"}, {'message': "Metric data"}]},
            {'_id': 0, 'meta.parameters.metricData.variables': 1}).sort('timestamp', -1).limit(1)

        temp_var = list(aggregation)

        if temp_var:
            aggregation = list(temp_var)[0]['meta']['parameters']['metricData']
        else:
            aggregation = {'variables': []}
    else:
        aggregation = {'variables': []}
    return aggregation


def auto_test(data_frame, sso, api, website, user, password, prefix, logger, db_var, dashboards,
              add_cookies, custom_headers, headers, var_type='default', keycloak=False):
    """
    Key function to start and poll metrics on selected instance via API

    :param (str) var_type: variables use from "logs" or "default"
    :param (pandas.core.frame.DataFrame) data_frame: existing metrics from prod
    :param (str) sso: url to sso
    :param (str) api: url to api
    :param (str) website: DNS
    :param user: username
    :param password: password
    :param (str) prefix: prefix like 'prod' or 'qa'
    :param logger: logger
    :param db_var: db for getting variables
    :param (str) dashboards: collection with dashboard
    :param (bool) add_cookies: if True then add '_currentUser' and 'roles' cookies
    :param (bool) custom_headers: if True then add custom headers
    :param (dict) headers: dictionary with custom headers
    :param (bool) keycloak: if keycloak authorization is active
    :return:
    """
    if keycloak:
        keycloak_openid = KeycloakOpenID(
            server_url=config.server_url,
            client_id=config.client_id,
            realm_name=config.realm_name,
            client_secret_key=config.client_secret_key)
        token = keycloak_openid.token(user.split('@')[0], password)
        headers = {'Authorization': 'Bearer {}'.format(str(token['access_token']))}
        session = requests.Session()
        session.headers.update(headers)
    else:
        session = run_sso_session(sso, user, password, logger, add_cookies, custom_headers, headers)
    result = pd.DataFrame()
    for i in range(0, data_frame.shape[0]):
        dashboard_id = data_frame.loc[i, 'dashboard_id']
        metric_id = data_frame.loc[i, 'metric_id']

        aggregation = collect_parameters(i, dashboard_id, metric_id, logger, user, db_var, dashboards, var_type)

        # формирование тела запроса метрики
        body = {"__content": {"type": "metricData",
                              "parameters": {"dashboardId": "{}".format(dashboard_id),
                                             "metricId": "{}".format(metric_id),
                                             "variables": aggregation['variables']}}}

        # отправка запроса
        try:
            start_job = session.post(api+'startJob', json=body, timeout=(12, 20))
        except requests.exceptions.Timeout:
            start_job = 'start_job_timeout'
            logger.info('{} at metric {}'.format(start_job, metric_id))
            result = write_results(result, i, dashboard_id, metric_id, str(start_job), 0, 400, website, prefix)
            sleep(120)
            break
        logger.info("metric {} run with code {} by {}".format(metric_id, int(start_job.status_code), user))
        if int(start_job.status_code) in [403, 500]:
            logger.info("metric {} failed with code {}".format(metric_id, int(start_job.status_code)))
            result = write_results(result, i, dashboard_id, metric_id, str(start_job.text), 0, int(start_job.status_code),
                website, prefix)
        elif int(start_job.status_code) in [404, 502]:
            logger.info("metric {} failed with code {}".format(metric_id, int(start_job.status_code)))
            result = write_results(result, i, dashboard_id, metric_id, str(start_job.text), 0, int(start_job.status_code),
                website, prefix)
            sleep(120)
        else:
            try:
                job = start_job.json()['id']
            except json.decoder.JSONDecodeError:
                try:
                    logger.info("metric {} failed with code {}".format(metric_id, int(start_job.status_code)))
                except ValueError:
                    logger.info("metric {} failed with code {}".format(metric_id, start_job.text))
                break

            # Получение ответа на запрос
            sleeper = time()
            poll_job = 'poll_job_timeout'
            while time()-sleeper <= 121:
                try:
                    poll_job = session.get(api+'pollJob/'+job, timeout=(12, 20))
                    logger.info("metric {} polled with code {}".format(metric_id, int(poll_job.status_code)))
                except requests.exceptions.ReadTimeout or requests.exceptions.ConnectionError:
                    logger.info('{} at metric {}'.format(poll_job, metric_id))
                    sleep(120)
                    break
                if int(poll_job.status_code) != 204:
                    logger.info("metric {} polled with code {} in {} sec".format(metric_id, int(poll_job.status_code),
                                                                                 int(time() - sleeper)))
                    break
                sleep(1)
            if poll_job == 'poll_job_timeout':
                text = poll_job
                status_code = 204
            else:
                text = read_poll_job(poll_job)
                status_code = int(poll_job.status_code)
            result = write_results(result, i, dashboard_id, metric_id, text, int(time()-sleeper), status_code, website,
                                   prefix)
    return result


def insert_test_results(db, collection, df, condition):
    """
    check for existing metric_id before updating - in order to prevent duplicates

    :param (pymongo.database.Database) db: DB name
    :param (str) collection: Collection name
    :param (pandas.core.frame.DataFrame) df: df with ids to be inserted
    :param (str) condition: attribute to merge ids on
    :return: None
    """
    unique_metrics = df[condition].unique().tolist()
    for instance in unique_metrics:
        for column in df.loc[df[condition] == instance]:
            db[collection].update_many({condition: instance},
                                       {'$set': {column: df.loc[df[condition] == instance][column].values[0],
                                                 'updated_time': datetime.utcnow()}},
                                       upsert=True)


def update_many(db, collection, condition, df, merger):
    unique_metrics = df[condition].unique().tolist()
    unique_columns = [column for column in df.columns.tolist() if column not in merger]
    for instance in unique_metrics:
        for column in df.loc[df[condition] == instance][unique_columns]:
            try:
                db[collection].update_many({condition: instance},
                                           {'$set': {column: str(df.loc[df[condition] == instance][column].values[0])}},
                                           upsert=True)
            except InvalidDocument:
                db[collection].update_many({condition: instance}, {'$set': {column: 'InvalidDocument'}}, upsert=True)
                break
            except WriteError:
                db[collection].update_many({condition: instance}, {'$set': {column: 'WriteError'}}, upsert=True)
                break


def run_multiple_tests(df_data, df_processor, merger, db_put, collection_put, logger, dfs, save_to_excel=False,
                       insert_to_mongo=False):
    """
    Run test -> merge with post-processor -> [merge with exceptions] -> save to excel | write to mongo

    :param (pandas.core.frame.DataFrame) df_data: existing metrics from prod
    :param (pandas.core.frame.DataFrame) df_processor: existing metrics with post-processor
    :param (str) merger: field to merge on
    :param db_put: db to put results of tests into
    :param collection_put: collections to put results of tests into
    :param logger: logger
    :param (dicts) dfs: configs for dfs to run tests on
    :param (bool) save_to_excel: if true then save to xlsx files
    :param (bool) insert_to_mongo: if true then update data in mongo collection
    :return: None
    """
    for df in dfs:
        df_result = auto_test(df_data, **df)
        if len(df_result.index)!=0:
            df_result = df_result.merge(df_processor, how='left', on=merger)
            df_result = df_result.rename(columns={'post_processor': 'post_processor_{}'.format(df['prefix'])})
            # merge with exceptions
        else:  # set up relevant errors
            df_result = pd.DataFrame()

        if save_to_excel:
            df_result.to_excel('auto_tests_{prefix}_{Time}.xlsx'.format(prefix=df['prefix'],
                Time=datetime.utcnow().strftime('%Y-%m-%d')), index=False)
            logger.info('results saved to excel')
        if insert_to_mongo:
            update_many(db_put, collection_put, 'metric_id', df_result, merger)
            logger.info('results inserted to mongo')


def get_credentials():
    l: str = getpass.getuser()

    parser = ArgumentParser()
    parser.add_argument('-l', '--login', nargs='?', dest='login', default=l,
                        type=str, help='Enter login if different from the system one')
    parser.add_argument('-p', '--password', nargs='?', dest='password', default=None,
                        help='Enter password')
    parser.add_argument('-t', '--test', dest='test_mode', action='store_false',
                        help='Add -t when running script if want to use full sample')
    parser.add_argument('-e', '--excel', dest='save_to_excel', action='store_true',
                        help='Add -e when running script if want to save results to excel file')
    parser.add_argument('-m', '--mongo', dest='insert_to_mongo', action='store_true',
                        help='Add -m when running script if want to insert to mongo')

    args = parser.parse_args()
    l = args.login
    p = args.password
    t: bool = args.test_mode
    e: bool = args.save_to_excel
    m: bool = args.insert_to_mongo
    if p is None:
        p: str = getpass.getpass()

    return l, p, t, e, m
