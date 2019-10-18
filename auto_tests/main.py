import sys, os
sys.path.append(os.path.abspath('.'))

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
import getpass

# TODO: доработать функционал сравнения результатов (с выделением нужных ключей через json.loads)
# TODO: логирование - прописать разграничение уровней ошибок
# TODO: настроить работу с data-files (обмен результатами логирования)
# TODO: переписать append'ы результата
# TODO: написать тест для запуска по меньшему числу метрик и убрать df.head()
# TODO: настроить сборку через tox.ini
# TODO: разобраться с Makefile и раскаткой через Travis.CI
# TODO: автоматическое версионирование (если возможно)
# TODO: написать алёрты на мониторинг выполнения скрипта
# TODO: переписать def main() корректно
# TODO: актуализировать setup.py, сделать автоматически
# TODO: логирование - прописать исполняющего скрипт пользователя
# TODO: логирование - оценить возможность развернуть ELK и складывать логи туда
# TODO: логирование - добавить факт наличия пост-обработчика в метрике

# set up logging
logging.basicConfig(filename='auto_tests.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def auto_test(data_frame, sso, api, website, user, password, prefix):
    # form names
    answer = 'answer_{}'.format(prefix)
    link = 'metric_{}_link'.format(prefix)
    code = 'code_{}'.format(prefix)
    result = pd.DataFrame(columns=['dashboard_id', 'metric_id', answer, link, code])

    # run session
    session = requests.Session()
    r = session.post(sso, data={"login": user, "password": password}, timeout=5)
    logger.info("session started with code {} by {}".format(int(r.status_code), user))
    session.cookies.set_cookie(requests.cookies.create_cookie('roles', 'admin'))
    session.cookies.set_cookie(requests.cookies.create_cookie('_currentUser', user))

    for i in range(0, data_frame.shape[0]):
        dashboard = data_frame.loc[i, 'dashboard']
        metrics_id = data_frame.loc[i, 'metrics_id']
        # сбор тела запроса метрики
        logger.info("job {} initiated by {}".format(int(i), user))
        pipeline_var = [{"$group": {"_id": "$metrics.{}.variables".format(data_frame.loc[i, 'metrics_id'])}},
                        {"$project": {"_id": 0, "variables": "$_id"}}]
        aggregation = functions.aggregate_var(config.db_qa, config.collection_dashboards, pipeline_var)
        logger.info("input variables for job {} are {}".format(int(i), aggregation['variables']))

        # формирование тела запроса метрики
        body = {"__content": {"type": "metricData",
                              "parameters": {"dashboardId": '{}'.format(dashboard),
                                             "metricId": '{}'.format(metrics_id),
                                             "variables": aggregation['variables']}}}

        start_job = session.post(api+'startJob', json=body, timeout=60)
        logger.info("job {} started with code {} by {}".format(int(i), int(start_job.status_code), user))
        if int(start_job.status_code) in [403, 500]:
            logger.info("job {} broke with code {}".format(int(i), start_job.status_code))
            temp = pd.DataFrame(data=[[dashboard, metrics_id, start_job.text, website+str(dashboard)+'/'+str(metrics_id),
                start_job.status_code]], index=[i], columns=['dashboard_id', 'metric_id', answer, link, code])
            result = result.append(temp)
        elif int(start_job.status_code) in [502]:
            logger.info("job {} failed with code {}".format(int(i), int(start_job.status_code)))
            temp = pd.DataFrame(data=[[dashboard, metrics_id, start_job.text, website+str(dashboard)+'/'+str(metrics_id),
                start_job.status_code]], index=[i], columns=['dashboard_id', 'metric_id', answer, link, code])
            result = result.append(temp)
            sleep(180)
        else:
            try:
                job = loads(start_job.text)['id']
            except JSONDecodeError:
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
            temp = pd.DataFrame(data=[[dashboard, metrics_id, start_job.text, website+str(dashboard)+'/'+str(metrics_id),
                start_job.status_code]], index=[i], columns=['dashboard_id', 'metric_id', answer, link, code])
            result = result.append(temp)
    return result


def main():
    df = data_coll.collect_data(config.db_prod, config.collection_dashboards, config.collection_collections,
                                mongo_scripts.pipeline_dash, config.black_list)
    df = df.head(5)
    login: str = getpass.getuser() + '@biocad.ru'
    password: str = getpass.getpass()
    df_answer_prod = auto_test(df, sso='https://sso.biocad.ru/login', api='https://analytics.biocad.ru/api/',
                               website='https://analytics.biocad.ru/dashboard/', user=login, password=password,
                               prefix='prod')
    df_answer_prod.to_excel('autotests_prod_{Time}.xlsx'.format(Time=strftime("%Y-%m-%d", gmtime())), index=False)
    df_answer_qa = auto_test(df, sso='https://stand.sso.biocad.ru/login', api='https://analytics-qa.biocad.ru/api/',
                             website='https://analytics-qa.biocad.ru/dashboard/', current_user=config.login,
                             prefix='qa')
    df_answer_qa.to_excel('autotests_qa_{Time}.xlsx'.format(Time=strftime("%Y-%m-%d", gmtime())), index=False)
    df_answer_all = df_answer_qa.merge(df_answer_prod, how='left', on=['dashboard_id', 'metric_id'])
    df_answer_all['correct'] = [*map(functions.compare_results, df_answer_all['answer_qa'], df_answer_all['answer_prod'])]
    df_answer_all.to_excel('autotests_all_{Time}.xlsx'.format(Time=strftime("%Y-%m-%d", gmtime())), index=False)
    return 'Done'


if __name__ == '__main__':
    main()
