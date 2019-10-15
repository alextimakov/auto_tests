import auto_tests.functions as functions
from pandas.io.json import json_normalize


def collect_data(db, collection_dashboards, collection_collections, pipeline, black_list):
    # Сбор dashboard_id, metrics_id с prod
    df_prod = functions.mongo_request(db, collection_dashboards, pipeline, 'metrics_id')

    # Собираем все модули
    cursor_collections = functions.read_mongo(db, collection_collections,
                                              query={"deleted": False}, output={'name.ru': 1})
    cursor_collections['name'] = json_normalize(cursor_collections['name'])
    collections = cursor_collections.loc[~cursor_collections['name'].isin(black_list)]['_id'].tolist()

    # собираем _id релевантных дашбордов, имя - для доппроверки # change to read_mongo
    needs_dashboards = functions.read_mongo(db, collection_dashboards, query={
        "$and": [{"deleted": False}, {"general.published": True}, {"general.collectionId": {"$in": collections}}]},
                                            output={'_id': 1, 'general.name.ru': 1})
    # fix this later
    for i in range(0, len(needs_dashboards)):
        needs_dashboards.loc[i, '_id'] = str(needs_dashboards.loc[i, '_id'])

    return df_prod.loc[df_prod['dashboard'].isin(needs_dashboards['_id'].unique())].reset_index(drop=True)
