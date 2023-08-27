import pandas as pd
import scrapy
import logging

from ..items import AmazonItem
import amazon.spiders.processing.read_yaml as read_yaml
from urllib.parse import urljoin


class AmazonBotSpider(scrapy.Spider):
    name = "amazon_bot"
    config = read_yaml.yaml_loader()
    input_path = '.'
    products = []

    # Load paths from configuration
    paths_data_frame = pd.read_csv(
        input_path + config['file_paths']['path_params'])
    paths_data_frame['path'] = input_path + \
        paths_data_frame['path'].astype(str)
    paths_dictionary = dict(paths_data_frame[['name', 'path']].values.tolist())

    # Read URLs from configuration
    urls_data_frame = pd.read_csv(paths_dictionary['urls'] + 'urls.csv')
    start_urls = urls_data_frame['url'].tolist()

    logging.info("Loaded start URLs for the spider.")

    # Extract base URL for pagination
    base_url = start_urls[0].split("&pg=", 1)[0]

    # Load CSS selectors from configuration
    css_data_frame = pd.read_csv(paths_dictionary['css'] + 'css.csv')
    css_selectors = dict(css_data_frame[['selector', 'css']].values.tolist())
    logging.info("Loaded CSS selectors for the spider.")

    def start_requests(self):
        self.count = 1
        logging.info("Starting requests using Splash.")
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                self.parse,
                meta={
                    'splash': {
                        'endpoint': 'render.html',
                        'args': {
                            'wait': 10,
                            'scroll': 5  # Simulate scroll for product loading
                        }
                    }
                }
            )

    def parse(self, response):
        logging.info(f"Parsing data from {response.url}")

        p_link = [urljoin('https://www.amazon.com/dp/', asin)
                  for asin in response.css(self.css_selectors['asin']).extract()]
        p_name = response.css(self.css_selectors['name']).css(
            '::text').extract()
        p_price = response.css(
            self.css_selectors['price']).css('::text').extract()
        p_rank = response.css(self.css_selectors['rank']).css(
            '::text').extract()
        p_asin = response.css(self.css_selectors['asin']).extract()

        logging.info(f"Extracted links: {len(p_link)}")
        logging.info(f"Extracted product names: {len(p_name)}")
        logging.info(f"Extracted ranks: {len(p_rank)}")
        logging.info(f"Extracted ASINs: {len(p_asin)}")

        for i in range(len(p_link)):
            item = {
                'link': p_link[i],
                'product_name': p_name[i],
                'price': p_price[i],
                'rank': p_rank[i],
                'asin': p_asin[i]
            }
            logging.debug(item)  # Using debug as this can be very verbose
            self.products.append(item)

        self.count += 1
        next_page = f'{self.base_url}&pg={self.count}'

        if self.count < 3:
            logging.info(f"Requesting next page: {next_page}")
            yield scrapy.Request(
                next_page,
                self.parse,
                meta={
                    'splash': {
                        'endpoint': 'render.html',
                        'args': {
                            'wait': 10,
                            'scroll': 5
                        }
                    }
                }
            )
        else:
            logging.warning("Reached max page count, stopping scraping.")

    def closed(self, reason):
        # This function is called when the spider is finished
        logging.info(f"Spider closed with reason: {reason}")
        # Convert the list of products to a pandas dataframe
        df = pd.DataFrame(self.products)
        logging.info("Converted scraped data to DataFrame.")

        # Save to a CSV or do whatever you want with the dataframe
        df.to_csv('scraped_products.csv', index=False)
        logging.info("Saved scraped data to 'scraped_products.csv'")
