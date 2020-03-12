import sys
import os
sys.path.append(os.path.abspath('.'))

import auto_tests.functions as functions
import auto_tests.config as config
import auto_tests.scripts as scripts
import logging.config

# TODO: логирование - прописать разграничение уровней ошибок (в logger.config)
# TODO: переписать append'ы результата на что-то более эффективное
# TODO: настроить сборку через tox.ini
# TODO: переписать def main() по-человечески
# TODO: актуализировать setup.py и сделать автоматический semver (возможно ли?)
# TODO: логирование - обмен результатами логирования, оценить возможность складывать логи в БД (всё же ELK?)
# TODO: проверять наличие прописанных exceptions в теле метрик с пост-обработчиком (df_processor)
# TODO: сделать нормальный фикс ошибки pymongo.errors.WriteError на больших ответах
# TODO: написать алёрты на мониторинг выполнения скрипта и рассылку результатов выполнения скрипта
# TODO: использовать асинхронные запросы или через мультипроцессинг
# TODO: изменить выбор defaultValue - в случае если defaultValueAsQuery = true
# TODO: добавить чёрный лист по дашбордам
# TODO: переписать на классы (класс "Авто тест")


# set up logging
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logger.config')
logging.config.fileConfig(log_file_path, disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def main():
    # credentials
    login, password, test_mode, save_to_excel, insert_to_mongo = functions.get_credentials()
    logger.info('test_mode: {}, save_to_excel: {}, insert_to_mongo: {}'.format(test_mode,
                                                                               save_to_excel, insert_to_mongo))
    login += config.mail

    # dfs with source data
    df = functions.collect_data(config.db_prod, config.collection_dashboards, config.collection_collections,
                                scripts.pipeline_dash, config.black_list_modules)
    df_processor = functions.mongo_request(config.db_prod, config.collection_dashboards, scripts.pipeline_processor)

    # dfs to run tests upon
    df_prod = {'sso': config.sso_prod_test, 'api': config.api_prod_test, 'website': config.site_prod_test,
                'user': login, 'password': password, 'prefix': 'prod_test', 'logger': logger,
                'db_var': config.db_prod, 'dashboards': config.collection_dashboards, 'add_cookies': False,
                'custom_headers': False, 'headers': {}, 'var_type': 'default', 'keycloak': True}
    df_prod_old = {'sso': config.sso_prod, 'api': config.api_prod, 'website': config.site_prod, 'user': login,
        'password': password, 'prefix': 'prod', 'logger': logger, 'db_var': config.db_prod,
        'dashboards': config.collection_dashboards, 'add_cookies': True, 'custom_headers': False, 'headers': {},
        'var_type': 'logs'}
    df_qa_old = {'sso': config.sso_qa, 'api': config.api_qa, 'website': config.site_qa, 'user': login,
        'password': password, 'prefix': 'qa', 'logger': logger, 'db_var': config.db_qa,
        'dashboards': config.collection_dashboards, 'add_cookies': True, 'custom_headers': False, 'headers': {},
        'var_type': 'default'}
    df_cluster_old = {'sso': config.sso_cluster, 'api': config.api_cluster, 'website': config.site_cluster,
        'user': config.user_cluster, 'password': config.password_cluster, 'prefix': 'cluster', 'logger': logger,
        'db_var': config.db_cluster, 'dashboards': config.collection_dashboards, 'add_cookies': True,
                  'custom_headers': True, 'headers': config.headers_cluster, 'var_type': 'default'}
    dfs = [df_prod]  #  df_cluster, df_prod_test, df_prod_test_2, df_qa

    # limit amount of source data for test
    if test_mode:
        df = df.iloc[:3, :].copy()
        df_processor = df_processor.iloc[:3, :].copy()

    # check for existing metric_id before updating
    if insert_to_mongo:
        functions.insert_test_results(config.db_prod, config.collection_auto_tests, df, config.merger[1])

    # run multiple tests
    functions.run_multiple_tests(df, df_processor, config.merger, config.db_prod, config.collection_auto_tests,
                                 logger, dfs, save_to_excel=save_to_excel, insert_to_mongo=insert_to_mongo)


if __name__ == '__main__':
    main()
