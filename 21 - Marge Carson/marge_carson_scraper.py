
import os
import json
import asyncio
from scrapy import Spider
from scrapy.http import Request
import time
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy import signals
from pydispatch import dispatcher
from bs4 import BeautifulSoup
import re


class MenuSpider(scrapy.Spider):
    name = 'menu_spider'
    
    custom_settings = {
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
        'CONCURRENT_REQUESTS': 1,
        'FEEDS': {
            'utilities/category-collection.json': {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf8',
            },
        },
    }

    start_urls = ['https://margecarson.com']

    def start_requests(self):
        output_dir = 'utilities'
        os.makedirs(output_dir, exist_ok=True)
        file_path = 'utilities/category-collection.json'
        with open(file_path, 'w') as file:
            pass
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse)

    def parse(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        furnitures_sub_data = soup.find("div", class_="mega-menu mega-menu--reverse")
        for category in furnitures_sub_data.find_all("li", class_="v-stack justify-items-start gap-5"):
            category_name = category.find("a", class_="h6").get_text().strip()
            sub_items = category.find("ul", class_="unstyled-list").find_all("li")
            for sub_item in sub_items:
                collection_name = sub_item.find("a").get_text().strip()
                collection_link = sub_item.find("a")["href"]
                if collection_name == "Custom Couture":
                    continue
                yield {
                    'category_name': category_name,
                    'collection_name': collection_name,
                    'collection_link': response.urljoin(collection_link),
                }
            
            
            
            

class CollectionSpider(scrapy.Spider):
    name = 'collection_spider'
    
    custom_settings = {
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
        'CONCURRENT_REQUESTS': 1,
        'FEEDS': {
            'utilities/products-links.json': {
                'format': 'json',
                'overwrite': True,
                'encoding': 'utf8',
            },
        },
    }

    def start_requests(self):
        output_dir = 'utilities'
        os.makedirs(output_dir, exist_ok=True)

        file_path = 'utilities/products-links.json'
        with open(file_path, 'w') as file:
            pass
        
        with open('utilities/category-collection.json') as file:
            self.collections = json.load(file)
        
        if self.collections:
            yield from self.process_collection(self.collections[0], 0)

    def process_collection(self, collection, collection_index):
        category_name = collection['category_name']
        collection_name = collection['collection_name']
        collection_link = collection['collection_link']
        
        yield scrapy.Request(
            url=collection_link,
            callback=self.parse_collection,
            meta={
                'category_name': category_name,
                'collection_name': collection_name,
                'collection_link': collection_link,
                'collection_index': collection_index 
            },
            dont_filter=True
        )

    def parse_collection(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')
        products = soup.find_all('product-card', class_  = "product-card")
        product_links = ["https://margecarson.com" + item.find('a').get('href') for item in products if item.find('a')]

        for link in product_links:
            yield {
                'category_name': response.meta['category_name'],
                'collection_name': response.meta['collection_name'],
                'product_link': link
            }
            
        next_page_link = soup.find('a', class_='pagination__link', rel='next')
        if next_page_link:
            next_page_url = "https://margecarson.com" + next_page_link.get('href')
            self.log(f"Following next page: {next_page_url}")
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse_collection,
                meta=response.meta,
                dont_filter=True
            )
        else:
            current_index = response.meta['collection_index']
            next_index = current_index + 1
            if next_index < len(self.collections):
                next_collection = self.collections[next_index]
                yield from self.process_collection(next_collection, next_index)
            else:
                self.log("All collections have been processed.")





class ProductSpider(scrapy.Spider):
    name = "product_spider"
    
    custom_settings = {
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
        'CONCURRENT_REQUESTS': 1
    }
    
    
    
    def start_requests(self):
        """Initial request handler."""
        os.makedirs('output', exist_ok=True)
        self.scraped_data = []
        scraped_links = set()
        if os.path.exists('output/products-data.json'):
            with open('output/products-data.json', 'r', encoding='utf-8') as f:
                try:
                    self.scraped_data = json.load(f)
                    scraped_links = {item['Product Link'] for item in self.scraped_data}
                except json.JSONDecodeError:
                    pass  
        with open('utilities/products-links.json', 'r', encoding='utf-8') as file:
            products = json.load(file)
        for product in products:
            product_link = product['product_link']
            if product_link not in scraped_links: 
                yield scrapy.Request(
                    url=product_link,
                    callback=self.parse,
                    meta={
                        'category_name': product['category_name'],
                        'collection_name': product['collection_name'],
                        'product_link': product['product_link']
                    }
                )
    
    def parse(self, response):
        """Parse the product page and extract details."""
        category_name = response.meta['category_name']
        collection_name = response.meta['collection_name']
        product_link = response.meta['product_link']
        
        soup = BeautifulSoup(response.text, 'html.parser')
        product_name = soup.find('h1', class_ = "product-title h2")
        if product_name:
            product_name = product_name.text.strip()
        sku = soup.select_one('div[data-block-id="sku_k9qwwH"] variant-sku')
        if sku:
            sku = sku.text.strip().replace("SKU:", "")
        vendor = soup.select_one('div[data-block-id="vendor"] a.vendor')
        if vendor:
            vendor = vendor.text.strip()
        
        product_images = []
        imgs = soup.find("div", class_ = 'product-gallery__image-list')
        if imgs:
            imgs = imgs.find_all("img")
            for item in imgs:
                product_images.append("https:" + item.get("src"))
        data = {}
        description = ""
        description_block = soup.find('div', {'data-block-id': 'description'})
        for p in description_block.find_all('p'):
            p_text = p.text.strip()
            
            if p_text.count(":") ==1:
                key, value = p_text.split(":", 1)
                data[key.strip()] = value.strip()
            else:
                p_text_parts = re.split(r'<br.*?>', p.decode_contents())
                for part in p_text_parts:
                    clean_part = BeautifulSoup(part, 'html.parser').get_text(strip=True)
                    
                    if ":" in clean_part:
                        parts = clean_part.split(":")
                        for i in range(0, len(parts) - 1, 2):
                            key = parts[i].strip()
                            value = parts[i + 1].strip()
                            data[key] = value
                    else:
                        description += clean_part.strip() + " "
        data = {k: v for k, v in data.items() if v}
        
        if description.strip():
            description = description.strip()
            
        data["Description"]= description
        
        
   

        new_product_data =  {
            'Category': category_name,
            'Collection': collection_name,
            'Product Link': product_link,
            'Product Title': product_name,
            'SKU': sku,
            "Vendor": vendor,
            'Product Images': product_images,
            'Specifications': data
        }
        
        self.scraped_data.append(new_product_data)
        
        with open('output/products-data.json', 'w', encoding='utf-8') as f:
            json.dump(self.scraped_data, f, ensure_ascii=False, indent=4)


    
    
    
#   -----------------------------------------------------------Run------------------------------------------------------------------------

def run_spiders():
    process = CrawlerProcess()
    # def run_menu_spider():
    #     process.crawl(MenuSpider)

    # def run_collection_spider():
    #     process.crawl(CollectionSpider)

    # def run_product_spider():
    #     process.crawl(ProductSpider)

    # def spider_closed(spider, reason):
    #     if isinstance(spider, MenuSpider):
    #         run_collection_spider()
    #     elif isinstance(spider, CollectionSpider):
    #         run_product_spider()

    # dispatcher.connect(spider_closed, signal=signals.spider_closed)

    # process.crawl(MenuSpider)
    process.crawl(ProductSpider)
    process.start()


run_spiders()