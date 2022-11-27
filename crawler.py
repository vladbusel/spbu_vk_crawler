import csv
from datetime import datetime
import random
import requests
import time

vk_token = 'vk_token'
version = 5.131
post_fields = ['id', 'owner_id', 'from_id', 'date', 'text', 'comments', 'likes', 'reposts', 'views']

start_time_str = "2021-11-12"
end_time_str = "2022-11-12"
max_posts_count = 5000000
queries = ['мгу', 'msu']
# group_list = ['spb1724',
#               'mmspbu',
#               'mobilityspbu',
#               'spbu.sport',
#               'spbu_love',
#               'overhearspbsu',
#               'spbuso',
#               'gsom.spbu',
#               'olympspbu',
#               'spbu_internship',
#               'spbuquote',
#               'ssspbu']
group_list = ['studsovetmsu',
               'overhear_msu',
               'msufootball',
               'ds_msu',
               'ustami_msu',
               'sportmsu',
               'msuprofcom',
               'olymp_lomonosov',
               'efmsu',
               'love_msu',
               'theatres_for_msu',
               'msu_public']
filename = 'requests_msu.csv'

feed_posts = []
post_ids = []


class VkApi:
    def __init__(self, token, version = 5.131):
        self.token = token
        self.version = version

    def newsfeed_search(self, query, start_time, end_time):
        requests_params = {
            'access_token': self.token,
            'v': self.version,
            'q': query,
            'count': 200,
            'start_time': start_time,
            'end_time': end_time
        }

        return requests.get('https://api.vk.com/method/newsfeed.search', params = requests_params)

    def groups_search(self, query):
        requests_params = {
            'access_token': self.token,
            'q': query,
            'count': 20
        }

        return requests.get('https://api.vk.com/method/groups.search', params = requests_params)

    def wall_get(self, domain, offset):
        requests_params = {
            'access_token': self.token,
            'v': self.version,
            'domain': domain,
            'count': 100,
            'offset': offset
        }
        return requests.get('https://api.vk.com/method/wall.get', params = requests_params)

def get_unix_time(date_time):
    return str(int(datetime.fromisoformat(date_time).timestamp()))

class VkCrawler:
    def __init__(self, vk_api, start_time, end_time = datetime.now(), file = None, max_posts_count = None):
        self.vk_api = vk_api
        self.start_time = get_unix_time(start_time)
        self.end_time = get_unix_time(end_time)
        self.max_posts_count = max_posts_count
        self.file = file
        self.feed_posts = []
        self.post_ids = []

    def post_fields():
        return ['id', 'owner_id', 'from_id', 'date', 'text', 'comments', 'likes', 'reposts', 'views']

    def get_by_query(self, query):
        last_record_end_time = self.end_time
        while self.max_posts_count is None or len(self.feed_posts) < self.max_posts_count:
            response = self.get_query_response(query, last_record_end_time)
            items = response['items']
            if len(items) == 0 or items[0]['date'] == last_record_end_time:
                break
            last_record_end_time = response['items'][-1]['date']
            time.sleep(random.uniform(0.1, 0.2)) 
            for post in items:
                self.add_post(post)
            last_record_datetime = datetime.fromtimestamp(last_record_end_time)
            print(f"{len(self.feed_posts)}\n{last_record_datetime}\n")

    def get_query_response(self, query, last_record_end_time):
        return self.vk_api.newsfeed_search(query, self.start_time, last_record_end_time).json()['response']

    def get_by_group(self, domain):
        offset = 0
        last_record_end_time = self.end_time
        while self.max_posts_count is None or len(self.feed_posts) < self.max_posts_count:
            response = self.get_group_response(domain, offset)
            items = response['items']
            if len(items) == 0 or (int(last_record_end_time) < int(self.start_time) or  items[0]['date'] == last_record_end_time):
                break
            last_record_end_time = items[-1]['date']
            time.sleep(random.uniform(0.1, 0.2))
            for post in items:
                self.add_post(post)
            offset += len(items)
            last_record_datetime = datetime.fromtimestamp(last_record_end_time)
            print(f"{len(self.feed_posts)}\n{last_record_datetime}\n")

    def get_group_response(self, domain, offset):
        return self.vk_api.wall_get(domain, offset).json()['response']

    def add_post(self, post):
        try:
            if (post["id"], post["owner_id"]) not in self.post_ids:
                if int(post['date']) > int(self.start_time) and int(post['date']) < int(self.end_time):
                    self.post_ids.append((post["id"], post["owner_id"]))
                    self.feed_posts.append(post_data(post))
                    if self.file is not None:
                        self.file.writerow(post_data(post))
        except:
                pass

def post_data(post):
    return (post['id'],
            post['owner_id'],
            post['from_id'],
            post['date'],
            post['text'],
            post['comments']['count'],
            post['likes']['count'],
            post['reposts']['count'],
            post['views']['count'],
            )

vk_api = VkApi(vk_token)
with open(filename, 'w', encoding="utf-8") as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(post_fields)
    vk_crawler = VkCrawler(vk_api, start_time_str, end_time_str, csv_writer)

    for query in queries:
        vk_crawler.get_by_query(query)
    
    for group in group_list:
        vk_crawler.get_by_group(group)

