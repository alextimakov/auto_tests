import sys, os
sys.path.append(os.path.abspath('.'))

import pandas as pd
import requests
import json
from time import sleep, time, strftime, gmtime
from datetime import datetime
from pandas.io.json import json_normalize
from bson import errors


def aggregate_dash(db, collection, pipeline):
    """
    Run aggregate query on selected collection from MongoDB and return it as Pandas DataFrame.

    :param (str) db: DB name
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

    :param (str) db: DB name
    :param (str) collection: Collection name
    :param (list) pipeline: each pipeline stage is a dict in format {"$method": "query"}
    :return (pandas.core.frame.DataFrame): returns DataFrame with queried data from selected collection
    """

    cursor = db[collection].aggregate(pipeline)
    result = list(cursor)[0]
    return result


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


def read_mongo(db, collection, query={}, output={}, no_id=False):
    """
    Read selected collection from MongoDB and return it as Pandas DataFrame.

    :param (str) db: DB name
    :param (str) collection: Collection name
    :param (dict) query: Query to documents in selected collection, like in db.collection.find(query)
    :param (dict) output: Select needed fields of selected documents
    :param (boolean) no_id: ObjectId if True else delete ObjectId
    :return (pandas.core.frame.DataFrame): returns DataFrame with queried data from selected collection
    """

    cursor = db[collection].find(query, output)  # Make a query to the specific DB and Collection
    df = pd.DataFrame(list(cursor))  # Expand the cursor and construct the DataFrame
    if no_id:
        del df['_id']
    return df


def write_results(final, i, dashboard, metrics_id, text, timer, status_code, website, prefix):
    result = pd.DataFrame(
        data=[[dashboard, metrics_id, text, timer, website+str(dashboard)+'/'+str(metrics_id), status_code]],
        index=[i],
        columns=['dashboard_id', 'metric_id', 'answer_{}'.format(prefix), 'timer', 'metric_{}_link'.format(prefix),
            'code_{}'.format(prefix)])
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
    return text


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
    for i in range(0, len(needs_dashboards)):
        needs_dashboards.loc[i, '_id'] = str(needs_dashboards.loc[i, '_id'])

    return df_prod.loc[df_prod['dashboard'].isin(needs_dashboards['_id'].unique())].reset_index(drop=True)


def run_sso_session(sso, user, password, logger, add_cookies=True, custom_headers=False, headers={}):
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


def auto_test(data_frame, sso, api, website, user, password, prefix, logger, db_qa, dashboards,
              add_cookies, custom_headers, headers):
    """
    Key function to start and poll metrics on selected instance via API

    :param (pandas.core.frame.DataFrame) data_frame: existing metrics from prod
    :param (str) sso: url to sso
    :param (str) api: url to api
    :param (str) website: dns
    :param user: username
    :param password: password
    :param (str) prefix: prefix like 'prod' or 'qa'
    :param logger: logger
    :param (pandas.core.frame.DataFrame) db_qa: db for getting variables from qa
    :param (str) dashboards: collection with dashboard
    :param (bool) add_cookies: if True then add '_currentUser' and 'roles' cookies
    :param (bool) custom_headers: if True then add custom headers
    :param (dict) headers: dictionary with custom headers
    :return:
    """
    session = run_sso_session(sso, user, password, logger, add_cookies, custom_headers, headers)
    result = pd.DataFrame()
    for i in range(0, data_frame.shape[0]):
        dashboard = data_frame.loc[i, 'dashboard']
        metric_id = data_frame.loc[i, 'metric_id']

        # сбор тела запроса метрики
        logger.info("job {} initiated by {}".format(int(i), user))
        pipeline_var = [{"$group": {"_id": "$metrics.{}.variables".format(metric_id)}},
            {"$project": {"_id": 0, "variables": "$_id"}}]
        aggregation = aggregate_var(db_qa, dashboards, pipeline_var)

        # формирование тела запроса метрики
        body = {"__content": {"type": "metricData",
                              "parameters": {"dashboardId": "{}".format(dashboard),
                                             "metricId": "{}".format(metric_id),
                                             "variables": aggregation['variables']}}}

        # отправка запроса
        try:
            start_job = session.post(api+'startJob', json=body, timeout=60)
        except requests.exceptions.Timeout:
            start_job = 'start_job_timeout'
            logger.info('{} at metric {}'.format(start_job, metric_id))
            result = write_results(result, i, dashboard, metric_id, str(start_job), 0, 400, website, prefix)
            break
        logger.info("metric {} run with code {} by {}".format(metric_id, int(start_job.status_code), user))
        if int(start_job.status_code) in [403, 500]:
            logger.info("metric {} failed with code {}".format(metric_id, int(start_job.status_code)))
            result = write_results(result, i, dashboard, metric_id, str(start_job.text), 0, int(start_job.status_code),
                website, prefix)
        elif int(start_job.status_code) in [502]:
            logger.info("metric {} failed with code {}".format(metric_id, int(start_job.status_code)))
            result = write_results(result, i, dashboard, metric_id, str(start_job.text), 0, int(start_job.status_code),
                website, prefix)
            sleep(180)
        else:
            try:
                job = json.loads(start_job.text)['id']
            except json.decoder.JSONDecodeError:
                try:
                    logger.info("metric {} failed with code {}".format(metric_id, int(start_job.status_code)))
                except ValueError:
                    logger.info("metric {} failed with code {}".format(metric_id, start_job.text))
                break

            # Получение ответа на запрос
            sleeper = time()
            while True:
                try:
                    poll_job = session.get(api+'pollJob/'+job, timeout=60)
                except requests.exceptions.ReadTimeout:
                    poll_job = 'poll_job_timeout'
                    logger.info('{} at metric {}'.format(poll_job, metric_id))
                    break
                logger.info("metric {} polled with code {}".format(metric_id, int(poll_job.status_code)))
                if poll_job.status_code != 204:
                    logger.info("metric {} polled with code {} in {} sec".format(metric_id, int(poll_job.status_code),
                        int(time()-sleeper)))
                    break
                sleep(1)
            if poll_job == 'poll_job_timeout':
                text = poll_job
            else:
                text = read_poll_job(poll_job)
            result = write_results(result, i, dashboard, metric_id, str(text), int(time()-sleeper),
                int(poll_job.status_code), website, prefix)
    return result


def insert_test_results(db, collection, df_list):
    df_list['updated_time'] = datetime.utcnow()
    db[collection].insert_many(df_list.T.to_dict().values(), ordered=False)


def update_many(db, collection, condition, df, merger):
    # NOT FINISHED !!!
    unique_metrics = df[condition].unique().tolist()
    unique_columns = [column for column in df.columns.tolist() if column not in merger]
    for instance in unique_metrics:
        for column in df.loc[df[condition] == instance][unique_columns]:
            try:
                db[collection].update_many({condition: instance},
                                           {'$set': {column: str(df.loc[df[condition] == instance][column].values[0])}})
            except errors.InvalidDocument:
               pass


def run_multiple_tests(df_data, df_processor, merger, db_put, collection_put, dfs, save_to_excel=False):
    """
    Run test -> merge with post-processor -> [merge with exceptions] -> save to excel | write to mongo

    :param (pandas.core.frame.DataFrame) df_data: existing metrics from prod
    :param (pandas.core.frame.DataFrame) df_processor: existing metrics with post-processor
    :param (str) merger: field to merge on
    :param db_put: db to put results of tests into
    :param collection_put: collections to put results of tests into
    :param (dicts) dfs: configs for dfs to run tests on
    :param (bool) save_to_excel: if true then save to xlsx files
    :return: None
    """
    for df in dfs:
        try:
            df_result = auto_test(df_data, **df)
            df_result = df_result.merge(df_processor, how='left', on=merger)
            df_result = df_result.rename(columns={'post_processor': 'post_processor_{}'.format(df['prefix'])})
            # merge with exceptions
        except ValueError or KeyError:  # set up relevant errors
            df_result = pd.DataFrame()
        if save_to_excel:
            df_result.to_excel('auto_tests_{prefix}_{Time}.xlsx'.format(prefix=df['prefix'],
                Time=strftime("%Y-%m-%d", gmtime())), index=False)
        update_many(db_put, collection_put, 'metric_id', df_result, merger)