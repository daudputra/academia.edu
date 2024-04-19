import scrapy
import json
from datetime import datetime
import os 
import requests

class SpiderSpider(scrapy.Spider):
    name = "spider"
    allowed_domains = ["www.academia.edu"]
    offset = 0
    start_urls = [f"https://www.academia.edu/v0/search/integrated_search?camelize_keys=true&canonical=true&fake_results=null&json=true&last_seen=null&offset={offset}&query=ensiklopedia&search_mode=works&size=10&sort=relevance&subdomain_param=api&user_language=en"]
    max_offset = 300


    def save_json(self, data, filename):
        with open(filename, 'w') as f:
            json.dump(data, f)

    def parse(self, response):
        data_string = response.body.decode('utf-8')
        data = json.loads(data_string)
        results = data['works']
        for result in results:
            work_id = result['id']
            title = result['title']
            created_at = result['createdAt']

            owner_id = result['ownerId']
            owner_name = result['owner']['displayName']
            owner_page_name = result['owner']['pageName']

            # pdf detail
            file_id = result['downloadableAttachments'][0]['id']
            file_name = result['downloadableAttachments'][0]['fileName']
            desc = result['translatedAbstract']
            file_download_link = result['downloadableAttachments'][0]['bulkDownloadUrl']
            page_count = result['pageCount']
            language = result['language']
            document_type = result['documentType']

            format_file = file_name.split('.')[-1]
            filename_clean = f'{title}_{work_id}'.replace('.', '_').replace('http://', '').replace(' ', '_').replace('"','').replace('|','').replace('__','_').replace(':','').replace('/','')
            filename = f'{filename_clean}.json'
            dir_raw = 'ensiklopedia'
            dir_path = os.path.join(os.getcwd(), dir_raw, 'json')
            os.makedirs(dir_path, exist_ok=True)
            s3_path = f's3://ai-pipeline-statistics/data/data_raw/ensiklopedia/Ensiklopedia di Academia/paper titles'
            data = {
                'domain' : 'https://www.academia.edu/',
                'link' : f'https://www.academia.edu/search?q={self.offset/10}ensiklopedia',
                'tags' : [
                    'https://impact.economist.com/',
                    'Paper Titles',
                    title
                ],
                'crawling_time' : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'crawling_time_epoch' : int(datetime.now().timestamp()),
                'path_data_raw' : s3_path,
                'path_data_clean' : None,
                'id' : work_id,
                'title' : title,
                'created_at' : created_at,
                'owner_id' : owner_id,
                'owner_name' : owner_name,
                'owner_page_name' : owner_page_name,
                'file_detail' : {
                    'file_id' : int(file_id),
                    # 'file_name' : file_name,
                    'file_name' : f'{filename_clean}.{format_file}',
                    'language' : language,
                    'page_count' : page_count,
                    'document_type' : document_type,
                    'desc' : desc,
                    'file_download_link' : file_download_link,
                }
            }
            self.save_json(data, os.path.join(dir_path, filename))
            self.download_pdf(file_download_link, format_file, filename_clean)


        self.offset += 10
        if self.offset <= self.max_offset:
            next_page = self.start_urls[0] + '&offset=' + str(self.offset)

            yield scrapy.Request(next_page, callback=self.parse)


    def download_pdf(self, url, format_file, filename_clean):
        path_pdf_raw = 'ensiklopedia'
        dir_pdf_raw = os.path.join(path_pdf_raw,'pdf')
        save_path = dir_pdf_raw
        os.makedirs(save_path, exist_ok=True)

        file_name_pdf = f'{filename_clean}.{format_file}'
        response = requests.get(url, verify=False)

        with open(os.path.join(save_path, file_name_pdf), 'wb') as f:
            f.write(response.content)