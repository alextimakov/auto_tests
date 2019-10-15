import auto_tests.data_coll as data_coll
import auto_tests.functions as functions
import auto_tests.config as config
import auto_tests.mongo_scripts as mongo_scripts
import requests
from json import loads
from time import sleep, gmtime, strftime, time
import pandas as pd
from json.decoder import JSONDecodeError
import logging
import urllib3
urllib3.disable_warnings()

# TODO: добавить функционал сравнения результатов (с выделением нужных ключей)
# TODO: переписать логирование (разграничение уровней ошибок)
# TODO: переписать append'ы результата

# set up logging
logging.basicConfig(filename='auto_tests.log', filemode='a',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def auto_test(data_frame, sso, api, website, current_user):
    result = pd.DataFrame()
    session = requests.Session()  # Открытие сессии
    r = session.post(sso, data={"login": config.login, "password": config.password}, timeout=5)
    logger.info("session started with code {}".format(int(r.status_code)))

    # cookies для запроса
    session.cookies.set_cookie(requests.cookies.create_cookie('roles', 'admin'))
    session.cookies.set_cookie(requests.cookies.create_cookie('_currentUser', current_user))

    for i in range(0, data_frame.shape[0]):
        # сбор тела запроса метрики
        logger.info("job {} initiated".format(int(i)))
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
            logger.info("job {} broke with code {}".format(int(i), start_job.status_code))
            result = result.append(pd.DataFrame([dict(
                zip(['dashboard_id', 'metric_id', 'answer_prod', 'metric_prod_link', 'code_prod'],
                    [data_frame.loc[i, 'dashboard'], data_frame.loc[i, 'metrics_id'], start_job.text,
                     website + str(data_frame.loc[i, 'dashboard']) + '/' + str(
                         data_frame.loc[i, 'metrics_id']), start_job.status_code]))]), ignore_index=True)
        elif int(start_job.status_code) in [502]:
            logger.info("job {} failed with code {}".format(int(i), int(start_job.status_code)))
            result = result.append(pd.DataFrame([dict(
                zip(['dashboard_id', 'metric_id', 'answer_prod', 'metric_prod_link', 'code_prod'],
                    [data_frame.loc[i, 'dashboard'], data_frame.loc[i, 'metrics_id'], start_job.text,
                     website+str(data_frame.loc[i, 'dashboard'])+'/'+str(data_frame.loc[i, 'metrics_id']),
                     start_job.status_code]))]), ignore_index=True)
            sleep(180)
        else:
            try:
                job = loads(start_job.text)['id']
            except JSONDecodeError:
                logger.info("mistake on {} with code {}".format(
                    website + str(data_frame.loc[i, 'dashboard']) + '/' + str(data_frame.loc[i, 'metrics_id']),
                    int(start_job.text)))
                break

            # Получение ответа на запрос
            sleeper = time()
            while True:
                poll_job = session.get(api+'pollJob/'+job, timeout=60)
                logger.info("job {} polled with code {}".format(int(i), int(poll_job.status_code)))
                if poll_job.status_code != 204:
                    break
                sleep(1)
            logger.info("job {} polled with code {} in {}".format(int(i),
                                                                  int(poll_job.status_code), int(sleeper-time())))
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
                               website='https://analytics.biocad.ru/dashboard/', current_user=config.login)
    df_answer_prod.to_excel('autotests_prod_{Time}.xlsx'.format(Time=strftime("%Y-%m-%d", gmtime())), index=False)
    df_answer_qa = auto_test(df, sso='https://stand.sso.biocad.ru/login', api='https://analytics-qa.biocad.ru/api/',
                             website='https://analytics-qa.biocad.ru/dashboard/', current_user=config.login)
    df_answer_qa.to_excel('autotests_qa_{Time}.xlsx'.format(Time=strftime("%Y-%m-%d", gmtime())), index=False)
    return 'Done'


if __name__ == '__main__':
    main()
