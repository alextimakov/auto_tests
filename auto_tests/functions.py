import pandas as pd
import requests
import json
from time import sleep, time


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


def write_results(final, i, dashboard, metrics_id, text, timer, job, website, prefix):
    result = pd.DataFrame(
        data=[[dashboard, metrics_id, text, timer, website+str(dashboard)+'/'+str(metrics_id), job.status_code]],
        index=[i],
        columns=['dashboard_id', 'metric_id', 'answer_{}'.format(prefix), 'timer', 'metric_{}_link'.format(prefix),
            'code_{}'.format(prefix)])
    final = final.append(result)
    return final


def compare_results(qa, prod):
    return 0 if str(qa) != str(prod) else 1


def run_sso_session(sso, user, password, logger, add_cookies=True):
    session = requests.Session()
    r = session.post(sso, data={"login": user, "password": password}, timeout=5)
    logger.info("session started with code {} by {}".format(int(r.status_code), user))
    if add_cookies:
        session.cookies.set_cookie(requests.cookies.create_cookie('roles', 'admin'))
        session.cookies.set_cookie(requests.cookies.create_cookie('_currentUser', user))
    else:
        pass
    return session


def auto_test(data_frame, sso, api, website, user, password, prefix, logger, db_qa, dashboards, add_cookies):
    session = run_sso_session(sso, user, password, logger, add_cookies)
    result = pd.DataFrame()
    for i in range(0, data_frame.shape[0]):
        dashboard = data_frame.loc[i, 'dashboard']
        metrics_id = data_frame.loc[i, 'metric_id']
        # сбор тела запроса метрики
        logger.info("job {} initiated by {}".format(int(i), user))
        pipeline_var = [{"$group": {"_id": "$metrics.{}.variables".format(data_frame.loc[i, 'metric_id'])}},
                        {"$project": {"_id": 0, "variables": "$_id"}}]
        aggregation = aggregate_var(db_qa, dashboards, pipeline_var)

        # формирование тела запроса метрики
        body = {"__content": {"type": "metricData",
                              "parameters": {"dashboardId": '{}'.format(dashboard),
                                             "metricId": '{}'.format(metrics_id),
                                             "variables": aggregation['variables']}}}

        start_job = session.post(api+'startJob', json=body, timeout=60)
        logger.info("job {} started with code {} by {}".format(int(i), int(start_job.status_code), user))
        if int(start_job.status_code) in [403, 500]:
            logger.info("job {} broke with code {}".format(int(i), start_job.status_code))
            result = write_results(result, i, dashboard, metrics_id, start_job.text, 0, start_job, website, prefix)
        elif int(start_job.status_code) in [502]:
            logger.info("job {} failed with code {}".format(int(i), int(start_job.status_code)))
            result = write_results(result, i, dashboard, metrics_id, start_job.text, 0, start_job, website, prefix)
            sleep(180)
        else:
            try:
                job = json.loads(start_job.text)['id']
            except json.decoder.JSONDecodeError:
                logger.info("mistake on {} with code {}".format(metrics_id, int(start_job.text)))
                break

            # Получение ответа на запрос
            sleeper = time()
            while True:
                poll_job = session.get(api+'pollJob/'+job, timeout=60)
                logger.info("job {} polled with code {}".format(int(i), int(poll_job.status_code)))
                if poll_job.status_code != 204:
                    break
                sleep(1)
            logger.info("job {} polled with code {} in {} sec for metric {}".format(int(i), int(poll_job.status_code),
                                                                                    int(time()-sleeper), metrics_id))
            # read poll_job results via json
            try:
                text = poll_job.json()['data']
            except ValueError:
                text = poll_job.text
            result = write_results(result, i, dashboard, metrics_id, text, int(time()-sleeper), poll_job,
                                             website, prefix)
    return result


def insert_test_results(db, collection, df_list):
    db[collection].insert_many(df_list)
