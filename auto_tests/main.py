import sys, os
sys.path.append(os.path.abspath('.'))

import auto_tests.data_coll as data_coll
import auto_tests.functions as functions
import auto_tests.config as config
import auto_tests.mongo_scripts as mongo_scripts
from datetime import datetime
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
# TODO: логирование - оценить возможность развернуть ELK и складывать логи туда
# TODO: писать результаты работы автотестов в БД
# TODO: проверять наличие прописанных exceptions в теле метрик с пост-обработчиком
# TODO: перевести список коллекций в словарь и дёргать их по ключам
# TODO: подсоединить ко всей это истории данные с кластера

# set up logging
logging.basicConfig(filename='auto_tests.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    df = data_coll.collect_data(config.db_prod, config.collection_dashboards, config.collection_collections,
                                mongo_scripts.pipeline_dash, config.black_list)
    df_processor = functions.mongo_request(config.db_prod, config.collection_dashboards,
                                           mongo_scripts.pipeline_processor)
    df = df.head(5)
    login: str = getpass.getuser() + config.mail
    password: str = getpass.getpass()

    df_prod = functions.auto_test(df, sso=config.sso_prod, api=config.api_prod, website=config.site_prod, user=login,
        password=password, prefix='prod', logger=logger, db_qa=config.db_qa, dashboards=config.collection_dashboards,
    add_cookies=True)
    df_prod = df_prod.merge(df_processor, how='left', on=config.merger)
    df_qa = functions.auto_test(df, sso=config.sso_qa, api=config.api_qa, website=config.site_qa, user=login,
        password=password, prefix='qa', logger=logger, db_qa=config.db_qa, dashboards=config.collection_dashboards,
    add_cookies=True)
    df_qa = df_qa.merge(df_processor, how='left', on=config.merger)
    df_all = df_qa.merge(df_prod, how='left', on=config.merger, suffixes=("_qa", "_prod"))
    df_all['correct'] = [*map(functions.compare_results, df_all['answer_qa'], df_all['answer_prod'])]
    df_all['updated_time'] = datetime.utcnow()
    functions.insert_test_results(config.db_prod, config.collection_auto_tests, df_all.T.to_dict().values())
    # df_cluster = functions.auto_test(df, sso=config.sso_cluster, api=config.api_cluster, website=config.site_cluster,
    #     user='axmor@biocad.ru', password=1, prefix='cluster', logger=logger, db_qa=config.db_qa,
    #     dashboards=config.collection_dashboards, add_cookies=False)
    # df_cluster = df_cluster.merge(df_processor, how='left', on=config.merger)
    # df_all = df_all.merge(df_cluster, how='left', on=config.merger)
    return 'Done'


if __name__ == '__main__':
    main()
