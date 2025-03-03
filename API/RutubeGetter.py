from bs4 import BeautifulSoup
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import requests
import re
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RutubeGetter:
    url_main_page = 'https://rutube.ru/feeds/top/page-'
    url_channel_page = 'https://rutube.ru/video/person/'
    videos_list = []
    channels_info = []
    videos_info = []
    default_timeout = 30
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) Chrome/78.0.3904.87 Safari/537.36"}
    
    def __init__(self, page_from = 1 , page_to = 300, max_workers=5):
        self.page_from = page_from
        self.page_to = page_to
        self.max_workers = max_workers
        self.videos_links = []
        self.channels_ids = set()
        self.aborted_videos = set()
        
        logger.info('Initializing parser')
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('--headless')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--window-size=1920,1080')
        self.options.add_argument('--ignore-certificate-errors')
        self.options.add_argument('--blink-settings=imagesEnabled=false')
        self.options.add_argument('--disable-blink-features=AutomationControlled')
        self.options.add_argument("--no-proxy-server")
        self.options.add_argument("--proxy-server='direct://'")
        self.options.add_argument("--proxy-bypass-list=*")
        
        self._parse_main_pages()
        self._fetch_video_info()
        self._fetch_channel_info()
        
    def _parse_main_pages(self):

        logger.info('Parsing main pages')
        driver = webdriver.Chrome(options=self.options)
        driver.implicitly_wait(5)
        
        for i in range(self.page_from, self.page_to + 1):
            try:
                driver.get(self.url_main_page + str(i) + '/')
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                page_source = WebDriverWait(driver, self.default_timeout).until(
                    lambda d: d.execute_script("return window.reduxState;")
                )
                videos = page_source['feed']['resources'][0]['items']
                for video in videos:
                    self.videos_links.append(video.get('video_url'))
                    self.channels_ids.add(video.get('author').get('id'))
                    self.videos_list.append(video)
                logger.info(f'Progress: {i / (self.page_to-self.page_from+1) * 100:.2f}%')
            except Exception as e:
                logger.error("Error parsing page %d: %s", i, e)
                break
        driver.quit()
        logger.info('Main pages parsed')
    
    def _fetch_video_info(self):

        logger.info('Fetching video info')
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(self._parse_video_page, self.videos_links)
    
    def _parse_video_page(self, url):
        
        try:
            driver = webdriver.Chrome(options=self.options)
            driver.get(url)
            WebDriverWait(driver, self.default_timeout).until(
               EC.visibility_of_element_located((By.CSS_SELECTOR, "button.wdp-video-like-dislike-reactions-module__reaction"))
            )
            page = driver.page_source
            driver.quit()
            
            soup = BeautifulSoup(page, 'html.parser')
            likes_count = int(soup.find('button', class_='wdp-video-like-dislike-reactions-module__reaction')['aria-label'].replace('\xa0', '').split(" ")[0])
            comments_count = int(soup.find('section', class_="wdp-comments-module__wrapper").find("h2").text.split(" ")[0])
            self.videos_info.append([url, likes_count, comments_count])
            logger.info(f'Processed video: {url}')
        except Exception as e:
            logger.error("Error processing video %s: %s. Restarting...", url, e)
            self.aborted_videos.add(url)
    
    def _fetch_channel_info(self):

        logger.info('Fetching channel info')
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(self._parse_channel_page, self.channels_ids)
    
    def _parse_channel_page(self, channel_id):

        try:
            redirected_url = requests.get(self.url_channel_page + str(channel_id)+'/', timeout=self.default_timeout, headers=self.headers).url
            main_page = BeautifulSoup(requests.get(redirected_url, timeout=self.default_timeout, headers=self.headers).text, 'html.parser')
            channel_info = BeautifulSoup(requests.get(redirected_url+'about/', timeout=self.default_timeout, headers=self.headers).text, 'html.parser')
            
            channel_info_text = channel_info.find('section', class_='wdp-wrapper-module__wdpCommonWrapper user-channel-module__tabWrapper')
            total_subs = int(channel_info_text.find('div', class_='wdp-user-channel-about-module__informationItemValue').text)
            channel_desc = channel_info_text.find('div', class_='wdp-user-channel-about-module__description').text.replace("\n", ' ')
            num_of_videos = int(re.search(r':\s*(\d+)\s*видео', main_page.find('title', {"data-react-helmet":"true"}).text).group(1))
            
            self.channels_info.append([channel_id, total_subs, channel_desc, num_of_videos])
            logger.info(f'Processed channel: {channel_id}')
        except Exception as e:
            logger.error("Error processing channel %s: %s", channel_id, e)
    
    def get_videos(self):
        video_info = []
        for i in range (len(self.videos_list)):
            data = [self.videos_list[i].get('author').get('id'),
                    self.videos_list[i].get('author').get('name'),
                    self.videos_list[i].get('category').get('name'),
                    self.videos_list[i].get('description'),
                    self.videos_list[i].get('duration'),
                    self.videos_list[i].get('hits'),
                    self.videos_list[i].get('publication_ts'),
                    self.videos_list[i].get('title'),
                    self.videos_list[i].get('video_url')]
            video_info.append(data)
    
        videos_rtb = pd.DataFrame(video_info, 
                    columns = ['author id',
                                'author name', 
                                'category name', 
                                'description', 
                                'duration',
                                'hits',
                                'publication_ts',
                                'title',
                                'video_url'])
        return videos_rtb
    
    def get_channels_info(self):
        ch_info_pd = pd.DataFrame(self.channels_info, columns = ['channel_id', 'total_subs', 'channel_desc','num_of_videos'])
        return ch_info_pd
    
    def get_videos_info(self):
        vid_info_df = pd.DataFrame(self.videos_info, columns = ["url", "likes_count", 'comments_count'])
        return vid_info_df
