import sys, os
sys.path.append(os.path.abspath('.'))

import auto_tests.functions as functions
import auto_tests.config as config
import auto_tests.scripts as scripts
import logging.config

# TODO: логирование - прописать разграничение уровней ошибок (в logger.config)
# TODO: переписать append'ы результата на что-то более эффективное
# TODO: настроить сборку через tox.ini
# TODO: написать алёрты на мониторинг выполнения скрипта
# TODO: переписать def main() по-человечески
# TODO: актуализировать setup.py и сделать автоматический semver (возможно ли?)
# TODO: логирование - обмен результатами логирования, оценить возможность складывать логи в БД (всё же ELK?)
# TODO: проверять наличие прописанных exceptions в теле метрик с пост-обработчиком (df_processor)
# TODO: перенести compare results в метрику
# TODO: сделать нормальный фикс ошибки pymongo.errors.WriteError на больших ответах
# TODO: настроить рассылку результатов выполнения скрипта
# TODO: использовать асинхронные запросы или через мультипроцессинг


# set up logging
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logger.config')
logging.config.fileConfig(log_file_path, disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def main():
    # credentials
    login, password, test_mode = functions.get_credentials()
    login += config.mail

    user_cluster: str = config.user_cluster
    password_cluster: str = config.password_cluster
    headers_cluster = config.headers_cluster

    # dfs with source data
    df = functions.collect_data(config.db_prod, config.collection_dashboards, config.collection_collections,
                                scripts.pipeline_dash, config.black_list)
    df_processor = functions.mongo_request(config.db_prod, config.collection_dashboards, scripts.pipeline_processor)

    # limit amount of source data for test
    if test_mode:
        df = df.head(3)
        df_processor = df_processor.head(3)

    # dfs to run tests upon
    df_prod = {'sso': config.sso_prod, 'api': config.api_prod, 'website': config.site_prod, 'user': login,
        'password': password, 'prefix': 'prod', 'logger': logger, 'db_qa': config.db_qa,
        'dashboards': config.collection_dashboards, 'add_cookies': True, 'custom_headers': False, 'headers': {}}
    df_qa = {'sso': config.sso_qa, 'api': config.api_qa, 'website': config.site_qa, 'user': login,
        'password': password, 'prefix': 'qa', 'logger': logger, 'db_qa': config.db_qa,
        'dashboards': config.collection_dashboards, 'add_cookies': True, 'custom_headers': False, 'headers': {}}
    df_cluster = {'sso': config.sso_cluster, 'api': config.api_cluster, 'website': config.site_cluster,
        'user': user_cluster, 'password': password_cluster, 'prefix': 'cluster', 'logger': logger,
        'db_qa': config.db_qa, 'dashboards': config.collection_dashboards, 'add_cookies': True, 'custom_headers': True,
        'headers': headers_cluster}
    dfs = [df_prod, df_qa, df_cluster]

    # check for existing metric_id before updating?
    functions.insert_test_results(config.db_prod, config.collection_auto_tests, df)

    # run multiple tests
    functions.run_multiple_tests(df, df_processor, config.merger, config.db_prod, config.collection_auto_tests, dfs,
                                 save_to_excel=False)


if __name__ == '__main__':
    main()
