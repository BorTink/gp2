import requests
import json

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class YouTubeGetter:
    __channel_ids = set()
    __channels = []
    __api_key = 'AIzaSyB0KFVbU7H6l-7sD3nlriOfyUkdns-L-uM'
    __country_codes = ['RU', 'AZ', 'AM', 'BY', 'KZ', 'MD', 'UA', 'LT', 'GE', 'LV', 'EE'] # можно добавить не-снг страны, дальше видео проверяется на русский язык
    __sng_country_codes = ['RU', 'AZ', 'AM', 'BY', 'KZ', 'MD', 'UA', 'LT', 'GE', 'LV', 'EE']
    __retrieved = {}
    __categories = {}
    __comment_threads = {}
    __playlists_cnt = {}

    def __get_curl(self, cc, pt, ca):
        scope = 'https://youtube.googleapis.com/youtube/v3/videos?'
        part = 'part=snippet%2Cstatistics'
        chart = 'chart=mostPopular'
        max_results = 'maxResults=50'
        region_code = f'regionCode={cc}'
        video_category = f'videoCategoryId={ca}'
        api_key = f'key={self.__api_key}'

        if pt == None:
            return scope + '&'.join([part, chart, max_results, region_code, video_category, api_key])

        page_token = f'pageToken={pt}'
        return scope + '&'.join([part, chart, max_results, page_token, region_code, video_category, api_key])
    
    def __get_curl_channels(self, chid):
        scope = 'https://youtube.googleapis.com/youtube/v3/channels?'
        part = 'part=snippet%2Cstatistics'
        channel_id = f'id={chid}'
        api_key = f'key={self.__api_key}'

        return scope + '&'.join([part, channel_id, api_key])

    def __get_curl_categories(self, cc):
        scope = 'https://youtube.googleapis.com/youtube/v3/videoCategories?'
        part = 'part=snippet'
        region_code = f'regionCode={cc}'
        api_key = f'key={self.__api_key}'

        return scope + '&'.join([part, region_code, api_key])

    def __get_curl_comment_threads(self, vid):
        scope = 'https://youtube.googleapis.com/youtube/v3/commentThreads?'
        part = 'part=snippet'
        video_id = f'videoId={vid}'
        api_key = f'key={self.__api_key}'

        return scope + '&'.join([part, video_id, api_key])

    def __get_curl_playlists(self, chid):
        scope = 'https://youtube.googleapis.com/youtube/v3/playlists?'
        part = 'part=content_details'
        channel_id = f'channelId={chid}'
        max_results = 'maxResults=1'
        api_key = f'key={self.__api_key}'

        return scope + '&'.join([part, channel_id, max_results, api_key])

    def __init__(self):
        for cc in self.__country_codes:
            self.__categories[cc] = []
            cur_curl = self.__get_curl_categories(cc)
            r = requests.get(cur_curl)
            j = r.json()
            self.__categories[cc].append(j['items'])
            cur_categories = [item['id'] for item in j['items']]

            self.__retrieved[cc] = []
            pt = None
            logger.info(f'progress: {round(100 * (self.__country_codes.index(cc) + 1) / len(self.__country_codes))}%')
            for ca in cur_categories:
                for i in range(4):
                    try:
                        cur_curl = self.__get_curl(cc, pt, ca)
                        r = requests.get(cur_curl)
                        j = r.json()
                        cur_items = []
                        for item in j['items']:
                            if cc in self.__sng_country_codes or ('defaultAudioLanguage' in item['snippet'].keys() and item['snippet']['defaultAudioLanguage'] == 'ru'):
                                cur_items.append(item)
                                self.__channel_ids.add(item['snippet']['channelId'])
                                vid = item['id']
                                comment_curl = self.__get_curl_comment_threads(vid)
                                comment_r = requests.get(comment_curl)
                                comment_j = comment_r.json()
                                self.__comment_threads[vid] = comment_j['items']
                        self.__retrieved[cc].append(cur_items)
                        pt = j['nextPageToken']
                    except:
                        break

        cnt = 1
        for chid in self.__channel_ids:
            if cnt == len(self.__channel_ids) // 2:
                logger.info('Половина каналов получена')
            cur_curl = self.__get_curl_channels(chid)
            r = requests.get(cur_curl)
            j = r.json()
            try:
                self.__channels.append(j['items'])
            except KeyError:
                logger.error(j['error'])
            cur_curl = self.__get_curl_playlists(chid)
            r = requests.get(cur_curl)
            j = r.json()
            self.__playlists_cnt[chid] = j['pageInfo']['totalResults']
            cnt += 1
            

    def get_videos(self):
        return self.__retrieved

    def get_channels(self):
        return self.__channels

    def get_categories(self):
        return self.__categories

    def get_comment_threads(self):
        return self.__comment_threads

    def get_playlists_cnt(self):
        return self.__playlists_cnt
