from auto_tests import data_coll, functions, config, mongo_scripts
import requests
from json import loads
from time import sleep, gmtime, strftime
import pandas as pd
from json.decoder import JSONDecodeError
import logging
import urllib3
urllib3.disable_warnings()

# TODO: добавить функционал сравнения результатов (с выделением нужных ключей)
# TODO: переписать логирование (разграничение уровней ошибок)

login = config.login
password = config.password

credentials = {
    "login": login,
    "password": password
}

# set up logging
logging.basicConfig(filename='auto_tests.log', filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def auto_test(data_frame, sso, api, website, current_user):
    result = pd.DataFrame()
    session = requests.Session()  # Открытие сессии
    r = session.post(sso, data=credentials, timeout=5)
    logger.info("session started with code {}".format(int(r.status_code)))

    # cookies для запроса
    session.cookies.set_cookie(requests.cookies.create_cookie('roles', 'admin'))
    session.cookies.set_cookie(requests.cookies.create_cookie('_currentUser', current_user))

    for i in range(0, data_frame.shape[0]):
        # сбор тела запроса метрики
        logger.info("job {} inititated".format(int(i)))
        pipeline_var = [{"$group": {"_id": "$metrics.{}.variables".format(data_frame.loc[i, 'metrics_id'])}},
                        {"$project": {"_id": 0, "variables": "$_id"}}]
        aggregation = functions.aggregate_var(config.db_qa, config.collection_dashboards, pipeline_var)

        # формирование тела запроса метрики
        body = {"__content": {"type": "metricData",
                              "parameters": {"dashboardId": '{}'.format(data_frame.loc[i, 'dashboard']),
                                             "metricId": '{}'.format(data_frame.loc[i, 'metrics_id']),
                                             "variables": aggregation['variables']}}}

        start_job = session.post(api+'startJob', json=body, timeout=60)
        logger.info("job {} started with code {}".format(int(i), int(start_job.status_code)))
        if int(start_job.status_code) in [403, 500]:
            result = result.append(pd.DataFrame([dict(
                zip(['dashboard_id', 'metric_id', 'answer_prod', 'metric_prod_link', 'code_prod'],
                    [data_frame.loc[i, 'dashboard'], data_frame.loc[i, 'metrics_id'], start_job.text,
                     website + str(data_frame.loc[i, 'dashboard']) + '/' + str(
                         data_frame.loc[i, 'metrics_id']), start_job.status_code]))]), ignore_index=True)
        else:
            try:
                job = loads(start_job.text)['id']
            except JSONDecodeError:
                logger.info("mistake on {} with code {}".format(
                    website + str(data_frame.loc[i, 'dashboard']) + '/' + str(data_frame.loc[i, 'metrics_id']),
                    int(start_job.text)))
                break

            # Получение ответа на запрос
            sleeper = 0
            while True:
                poll_job = session.get(api+'pollJob/'+job, timeout=60)
                logger.info("job {} polled with code {} in {}".format(int(i), int(start_job.status_code), int(sleeper)))
                if poll_job.status_code != 204:
                    break
                sleeper += 1
                sleep(1)

            result = result.append(pd.DataFrame([dict(
                zip(['dashboard_id', 'metric_id', 'answer_prod', 'metric_prod_link', 'code_prod'],
                    [data_frame.loc[i, 'dashboard'], data_frame.loc[i, 'metrics_id'], poll_job.text,
                     website + str(data_frame.loc[i, 'dashboard']) + '/' + str(
                         data_frame.loc[i, 'metrics_id']), poll_job.status_code]))]), ignore_index=True)
    return result


def main():
    df = data_coll.collect_data(config.db_prod, config.collection_dashboards, config.collection_collections,
                        mongo_scripts.pipeline_dash, config.black_list)
    df_answer_prod = auto_test(df, sso='https://sso.biocad.ru/login', api='https://analytics.biocad.ru/api/',
                               website='https://analytics.biocad.ru/dashboard/', current_user=login)
    df_answer_prod.to_excel('autotests_prod_{Time}.xlsx'.format(Time=strftime("%Y-%m-%d", gmtime())), index=False)
    df_answer_qa = auto_test(df, sso='https://stand.sso.biocad.ru/login', api='https://analytics-qa.biocad.ru/api/',
                             website='https://analytics-qa.biocad.ru/dashboard/', current_user=login)
    df_answer_qa.to_excel('autotests_prod_{Time}.xlsx'.format(Time=strftime("%Y-%m-%d", gmtime())), index=False)
    return 'Done'


if __name__ == '__main__':
    main()
