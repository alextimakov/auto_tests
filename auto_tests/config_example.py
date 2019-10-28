from pymongo import MongoClient

# mongo prod
client_prod = MongoClient('mongodb://user:password@ip:port')
db_prod = client_prod['db_name']

# mongo qa
client_qa = MongoClient('mongodb://user:password@ip:port')
db_qa = client_qa['db_name']

# mongo cluster
client_cluster = MongoClient('mongodb://user:password@ip:port')
db_cluster = client_cluster['db_name']

# collections
collection_dashboards: str = 'collection'
collection_collections: str = 'collection'
collection_auto_tests: str = 'collection'

# URLs
sso_prod = 'https://url/login/'
api_prod = 'https://url/api/'
site_prod = 'https://url/'

sso_qa = 'https://url/login/'
api_qa = 'https://url/api/'
site_qa = 'https://url/'

sso_cluster = 'https://url/login/'
api_cluster = 'https://url/api/'
site_cluster = 'https://url/'
origin_cluster = 'https://url/'

# custom variables
mail: str = '@mail.domain'
merger: list = ['param', 'param']
black_list: list = ['collection', 'collection']

# temp variables
user_cluster: str = 'user'
password_cluster = 'password'