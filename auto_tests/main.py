import sys, os
sys.path.append(os.path.abspath('.'))

import auto_tests.functions as functions
import auto_tests.config as config
import auto_tests.scripts as scripts
import logging
import logging.config as logging_config
import getpass

# TODO: логирование - прописать разграничение уровней ошибок (в logger.config)
# TODO: переписать append'ы результата
# TODO: написать тест для запуска по меньшему числу метрик и убрать df.head()
# TODO: настроить сборку через tox.ini
# TODO: разобраться с Makefile и раскаткой через Travis.CI
# TODO: написать алёрты на мониторинг выполнения скрипта
# TODO: переписать def main() корректно
# TODO: актуализировать setup.py и сделать автоматический semver
# TODO: логирование - обмен результатами логирования, оценить возможность развернуть ELK и складывать логи туда
# TODO: проверять наличие прописанных exceptions в теле метрик с пост-обработчиком
# TODO: перевести список коллекций в словарь и дёргать их по ключам
# TODO: переписать единый insert на несколько поступательных update
# TODO: перенести compare results в метрику

# set up logging
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logger.config')
logging_config.fileConfig(log_file_path, disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def main():
    df = functions.collect_data(config.db_prod, config.collection_dashboards, config.collection_collections,
                                scripts.pipeline_dash, config.black_list)
    functions.insert_test_results(config.db_prod, config.collection_auto_tests, df)
    df_processor = functions.mongo_request(config.db_prod, config.collection_dashboards,
                                           scripts.pipeline_processor)
    # df = df.head(5)

    # credentials configuration
    login: str = getpass.getuser() + config.mail
    password: str = getpass.getpass()

    user_cluster = config.user_cluster
    password_cluster = config.password_cluster
    headers_cluster = config.headers_cluster

    # dfs configuration
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

    # run auto tests
    functions.run_multiple_tests(df, df_processor, config.merger, config.db_prod, config.collection_auto_tests, dfs)
    return 'Done'


if __name__ == '__main__':
    main()
