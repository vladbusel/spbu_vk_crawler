import unittest
from unittest.mock import Mock
from primer import VkCrawler, VkApi
from datetime import datetime
import pandas as pd

class MockVkApi:
    def __init__(self):
        pass

    def newsfeed_search(self):
        pass

    def wall_get(self):
        pass

class VkCrawlerTests(unittest.TestCase):
    def setUp(self):
        self.vk_api = MockVkApi()
        self.start_time_str = "2022-11-11"
        self.end_time_str = "2022-11-12"
        return_value = {'items': [{'id': 'id', 'owner_id': 'owner_id',
                                  'from_id': 'from_id', 'date': 'date',
                                  'text': 'text', 'comments': { 'count': 'comments_count'},
                                  'likes': { 'count': 'likes_count'}, 'reposts': { 'count': 'reposts_count'},
                                  'views': { 'count': 'views_count'}, 'date': 1668158577
                                  }]}
        self.subject = VkCrawler(self.vk_api, self.start_time_str, self.end_time_str)
        self.subject.get_query_response = Mock(return_value=return_value)
        self.subject.get_group_response = Mock(return_value=return_value)

    def test_max_count(self):
        self.subject = VkCrawler(self.vk_api, self.start_time_str, self.end_time_str, max_posts_count = 0)
        self.subject.get_by_query('query')
        self.assertEqual(len(self.subject.feed_posts), 0)

    def test_get_by_query_count(self):
        self.subject.get_by_query('query')
        self.assertEqual(len(self.subject.feed_posts), 1)

    def test_get_by_query_instance(self):
        self.subject.get_by_query('query')
        self.assertEqual(self.subject.feed_posts[0],
                        ('id','owner_id','from_id',1668158577,'text','comments_count','likes_count','reposts_count','views_count'))

    def test_get_by_query_duplicate(self):
        item = {'id': 'id', 'owner_id': 'owner_id',
                'from_id': 'from_id', 'date': 'date',
                'text': 'text', 'comments': { 'count': 'comments_count'},
                'likes': { 'count': 'likes_count'}, 'reposts': { 'count': 'reposts_count'},
                'views': { 'count': 'views_count'}, 'date': 1668158577
                }
        return_value = {'items': [item, item]}
        self.subject.get_query_response = Mock(return_value=return_value)
        self.subject.get_by_query('query')
        self.assertEqual(len(self.subject.feed_posts), 1)

    def test_get_by_query_with_empty_response(self):
        self.subject.get_query_response = Mock(return_value= { 'items': [] })
        self.subject.get_by_query('query')
        self.assertEqual(len(self.subject.feed_posts), 0)

    def test_get_by_group_count(self):
        self.subject.get_by_group('group')
        self.assertEqual(len(self.subject.feed_posts), 1)

    def test_get_by_group_instance(self):
        self.subject.get_by_group('group')
        self.assertEqual(self.subject.feed_posts[0],
                        ('id','owner_id','from_id',1668158577,'text','comments_count','likes_count','reposts_count','views_count'))

    def test_get_by_group_duplicate(self):
        item = {'id': 'id', 'owner_id': 'owner_id',
                'from_id': 'from_id', 'date': 'date',
                'text': 'text', 'comments': { 'count': 'comments_count'},
                'likes': { 'count': 'likes_count'}, 'reposts': { 'count': 'reposts_count'},
                'views': { 'count': 'views_count'}, 'date': 1668158577
                }
        return_value = {'items': [item, item]}
        self.subject.get_group_response = Mock(return_value=return_value)
        self.subject.get_by_group('group')
        self.assertEqual(len(self.subject.feed_posts), 1)

    def test_get_by_group_with_empty_response(self):
        self.subject.get_group_response = Mock(return_value= { 'items': [] })
        self.subject.get_by_group('group')
        self.assertEqual(len(self.subject.feed_posts), 0)

    def test_get_by_group_with_date_out_of_range(self):
        date = str(int(datetime.fromisoformat(self.end_time_str).timestamp()) + 1) 
        self.subject.get_group_response = Mock(return_value= { 'items': [{'id': 'id', 'owner_id': 'owner_id',
                                                                          'from_id': 'from_id', 'date': 'date',
                                                                          'text': 'text', 'comments': { 'count': 'comments_count'},
                                                                          'likes': { 'count': 'likes_count'}, 'reposts': { 'count': 'reposts_count'},
                                                                          'views': { 'count': 'views_count'}, 'date': date
                                                                          }] })
        self.subject.get_by_group('group')
        self.assertEqual(len(self.subject.feed_posts), 0)

    def test_add_incorrect_post(self):
      post = {'id': 'id'}
      self.subject.add_post(post)
      self.assertEqual(len(self.subject.feed_posts), 0)


class VkApiTests(unittest.TestCase):
    def setUp(self):
        self.vk_api = VkApi('0da61acf0da61acf0da61acf860eb75aa400da60da61acf6efb30a669036cf0c17b8662')
        self.start_time_str = str(int(datetime.fromisoformat("2022-11-11").timestamp()))
        self.end_time_str = str(int(datetime.fromisoformat("2022-11-12").timestamp()))
        self.query_response = self.vk_api.newsfeed_search('spbu', self.start_time_str, self.end_time_str)
        self.group_response = self.vk_api.wall_get('spb1724', 0)

    def test_newsfeed_search(self):
        response = self.vk_api.newsfeed_search('spbu', self.start_time_str, self.end_time_str)
        self.assertEqual(response.status_code, 200)

    def test_newsfeed_search_response_body(self):
        response = self.query_response.json()
        self.assertEqual(list(response.keys()), ['response'])
        attr_list = ['items']
        self.assertTrue(all(attr in response['response'].keys() for attr in attr_list))

    def test_newsfeed_search_response_items(self):
        response = self.query_response.json()
        self.assertIsInstance(response['response']['items'], list)
        attr_list = ['id', 'owner_id', 'from_id', 'views', 'text',
                     'likes', 'reposts', 'date', 'comments']
        self.assertTrue(all(attr in response['response']['items'][0].keys() for attr in attr_list))

    def test_newsfeed_search_max_time_range(self):
        response = self.query_response.json()
        self.assertLessEqual(response['response']['items'][0]['date'], int(self.end_time_str))

    def test_newsfeed_search_min_time_range(self):
        response = self.query_response.json()
        self.assertGreaterEqual(response['response']['items'][-1]['date'], int(self.start_time_str))

    def test_wall_get(self):
        response = self.vk_api.wall_get('spb1724', 0)
        self.assertEqual(response.status_code, 200)

    def test_wall_get_response_body(self):
        response = self.group_response.json()
        self.assertEqual(list(response.keys()), ['response'])
        attr_list = ['items']
        self.assertTrue(all(attr in response['response'].keys() for attr in attr_list))

    def test_wall_get_response_items(self):
        response = self.group_response.json()
        self.assertIsInstance(response['response']['items'], list)
        attr_list = ['id', 'owner_id', 'from_id', 'views', 'text',
                     'likes', 'reposts', 'date', 'comments']
        # attr_list = ['id', 'owner_id', 'from_id', 'attachments', 'views', 'text', 'post_source', 'post_type', 'marked_as_ads',
        #              'hash', 'likes', 'donut', 'reposts', 'short_text_rate', 'date', 'comments']
        self.assertTrue(all(attr in response['response']['items'][0].keys() for attr in attr_list))


class StatisticTest(unittest.TestCase):
    def setUp(self):
        self.df = pd.read_csv('requests_test.csv', encoding='utf-8')
        self.df.dropna(axis=1)

    def test_posts_per_day(self):
        df = self.df
        df["date"] = df["date"].apply(pd.Timestamp.fromtimestamp)
        counts = df.groupby(df["date"].apply(lambda x: x.date()))["id"].count()
        self.assertEqual(list(counts[0:3]), [6, 2, 2])
    
    def test_views_per_day(self):
        df = self.df
        df["date"] = df["date"].apply(pd.Timestamp.fromtimestamp)
        views = df.groupby(df["date"].apply(lambda x: x.date()))["views"].sum()
        self.assertEqual(list(views[0:3]), [1507, 1609, 244])

    def test_mean_views_per_day(self):
        df = self.df
        df["date"] = df["date"].apply(pd.Timestamp.fromtimestamp)
        views = df.groupby(df["date"].apply(lambda x: x.date()))["views"].mean()
        self.assertEqual(list(views[0:3]), [251.16666666666666, 804.5, 122.0])

    def test_median_views_per_day(self):
        df = self.df
        df["date"] = df["date"].apply(pd.Timestamp.fromtimestamp)
        views = df.groupby(df["date"].apply(lambda x: x.date()))["views"].median()
        self.assertEqual(list(views[0:3]), [311.5, 804.5, 122.0])

    def test_unique_users_count(self):
        unique_users = len(self.df['owner_id'].value_counts().index)
        self.assertEqual(unique_users, 9)

    def test_attributes_sum(self):
        self.assertEqual(list(self.df[["likes", "reposts", "comments", "views"]].sum()), [122, 11, 10, 3360])
