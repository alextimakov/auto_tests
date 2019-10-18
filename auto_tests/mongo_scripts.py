# pipelines
pipeline_dash = [{"$match": {"$and": [{"deleted": {"$ne": True}}, {"general.published": {"$ne": False}}]}},
    {"$project": {"id": "$_id", "published": "$general.published", "variable": {"$objectToArray": "$metrics"}}},
    {"$match": {"$and": [{"deleted": {"$ne": True}}, {"general.published": {"$ne": False}}]}},
    {"$project": {"dashboard": "$id", "metrics_id": "$variable.k", "metrics_del": "$variable.v.deleted",
            "metrics_pub": "$variable.v.general.published.isPublished"}},
    {"$unwind": {"path": "$metrics_id", "includeArrayIndex": "metric_id_index"}},
    {"$unwind": {"path": "$metrics_del", "includeArrayIndex": "metric_del_index"}},
    {"$unwind": {"path": "$metrics_pub", "includeArrayIndex": "metric_pub_index"}},
    {"$project": {"dashboard": 1, "metrics_id": 1, "metrics_del": 1, "metrics_pub": 1,
            "compare_del": {"$cmp": ["$metric_id_index", "$metric_del_index"]},
            "compare_pub": {"$cmp": ["$metric_id_index", "$metric_pub_index"]}}},
    {"$match": {"$and": [{"metrics_del": False}, {"metrics_pub": True}, {"compare_del": 0}, {"compare_pub": 0}]}},
    {"$project": {"dashboard": 1, "metrics_id": 1, "_id": 0}}]

pipeline_processor = [{"$match": {"deleted": {"$ne": True}}},
    {"$project": {"id": "$_id", "dashboard": "$general.name.ru", "metrics": {"$objectToArray": "$metrics"}}},
    {"$project": {"dashboard_id": "$_id", "metric_id": "$metrics.k", "post_processor": "$metrics.v.postProcessor.active"}},
    {"$unwind": {"path": "$metric_id", "includeArrayIndex": "metric_id_index"}},
    {"$unwind": {"path": "$post_processor", "includeArrayIndex": "processor_index"}},
    {"$project": {"dashboard_id": 1, "metric_id": 1, "post_processor": 1,
            "compare": {"$cmp": ["$metric_id_index", "$processor_index"]}}},
    {"$match": {"compare": 0}},
    {"$project": {"dashboard_id": 1, "metric_id": 1, "post_processor": 1, '_id': 0}}]
