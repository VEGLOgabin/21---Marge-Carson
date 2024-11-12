
from twisted.internet import asyncioreactor
asyncioreactor.install()
import os
import json
import asyncio
from scrapy import Spider
from scrapy.http import Request
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
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
            'Print Tearsheet': None,
            'Specifications': data
        }
        
        self.scraped_data.append(new_product_data)
        
        with open('output/products-data.json', 'w', encoding='utf-8') as f:
            json.dump(self.scraped_data, f, ensure_ascii=False, indent=4)


class ProductPrintSheetDownloaderSpider(Spider):
    name = "product_spider"
    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': False,
            'timeout': 100000,
        },
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
        'CONCURRENT_REQUESTS': 1,
        'LOG_LEVEL': 'INFO',
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'HTTPERROR_ALLOW_ALL': True,
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' \
                        'AppleWebKit/537.36 (KHTML, like Gecko) ' \
                        'Chrome/115.0.0.0 Safari/537.36',
            'Accept-Language': 'en',
        },
    }
    
    def start_requests(self): 
        """Start requests by reading product data and skipping those with existing Print Tearsheet."""
        file_path = 'output/products-data.json'
        if not os.path.exists(file_path):
            self.logger.error(f"{file_path} does not exist. Exiting spider.")
            return
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                self.products = json.load(file)
        except json.JSONDecodeError as e:
            self.logger.error(f"Error loading JSON file: {e}")
            return
        scraped_product_links = {item['Product Link'] for item in self.products if item["Print Tearsheet"]}
        for product in self.products:
            product_link = product.get('Product Link')
            print_sheet = product.get("Print Tearsheet")
            category_name = product.get('Category')
            collection_name = product.get('Collection')
            if not print_sheet and product_link:
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                if product_link in scraped_product_links:
                    scraped_product = next((item for item in self.products if item['Product Link'] == product_link), None)
                    if scraped_product:
                        if collection_name not in scraped_product['Collection'] or category_name not in scraped_product['Category']:
                            new_product_data = scraped_product.copy()
                            new_product_data['Collection'] = collection_name
                            new_product_data['Category'] = category_name
                            self.save_updated_product(new_product_data)
                            self.logger.info(f"Updated product with new collection or category: {product_link}")
                    else:
                        self.logger.warning(f"Product link found in scraped_product_links but not in scraped_data: {product_link}")
                else:
                    yield Request(
                        url=product_link,
                        meta={
                            'playwright': True,
                            'playwright_include_page': True,
                            'product': product
                        },
                        callback=self.parse,
                        errback=self.handle_error
                    )
                
                
    
    
    async def parse(self, response):
        """Parse the product page using Playwright and extract details."""
        product = response.meta['product']
        page = response.meta['playwright_page']
        try:
            await page.wait_for_selector('div#myBtn-litpdf', timeout=5000)
            await page.locator("div#myBtn-litpdf").click()
            await page.wait_for_selector('div[data-action="download"]', timeout=5000)
            context = page.context
            new_page_promise = context.wait_for_event('page')
            await page.locator('//*[@id="myModal-litpdf"]/div/div[2]').click()
            new_page = await new_page_promise
            await new_page.wait_for_load_state()
            # while "filename=" in new_page.url:
            #     await asyncio.sleep(1)
            new_page_url = new_page.url
            if new_page_url:
                await page.goto(new_page_url)
                # new_page_url = page.url
                while "filename=" in page.url:
                    await asyncio.sleep(1)
                new_page_url = page.url
                self.logger.info(f"New Page URL: {new_page_url}")
                product['Print Tearsheet'] = new_page_url
                self.save_updated_product(product)

        except PlaywrightTimeoutError:
            self.logger.warning("Timeout reached while waiting for selector. Skipping...")
            return
            
        finally:
            await page.close()
              
    def save_updated_product(self, updated_product):
        """Update the product data back to the JSON file."""
        file_path = 'output/products-data.json'
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                products = json.load(file)
        except json.JSONDecodeError as e:
            self.logger.error(f"Error loading JSON file: {e}")
            return
        updated_product_key = (updated_product['Product Link'], updated_product["Collection"], updated_product['Category'])
        for i, product in enumerate(products):
            product_data = (product['Product Link'], product["Collection"], product['Category'])
            if product_data == updated_product_key:
                products[i] = updated_product
                self.logger.warning(f"Product at : {product['Product Link']} updated successfully")
                break
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(products, file, ensure_ascii=False, indent=4)

    def handle_error(self, failure):
        """Handle request errors."""
        self.logger.error(f"Request failed: {failure}")
        
           
    
    
    
#   -----------------------------------------------------------Run------------------------------------------------------------------------

def run_spiders():
    process = CrawlerProcess()
    def run_menu_spider():
        process.crawl(MenuSpider)

    def run_collection_spider():
        process.crawl(CollectionSpider)

    def run_product_spider():
        process.crawl(ProductSpider)
        
    def run_print_sheet_downloader():
        process.crawl(ProductPrintSheetDownloaderSpider)

    def spider_closed(spider, reason):
        if isinstance(spider, MenuSpider):
            run_collection_spider()
        elif isinstance(spider, CollectionSpider):
            run_product_spider()
        elif isinstance(spider, ProductSpider):
            run_print_sheet_downloader()

    dispatcher.connect(spider_closed, signal=signals.spider_closed)

    process.crawl(MenuSpider)
    process.start()


run_spiders()