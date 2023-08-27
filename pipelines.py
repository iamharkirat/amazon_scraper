# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


import pandas as pd


class AmazonPipeline:
    def __init__(self):
        # Initialize an empty list to collect the items
        self.items = []

    def process_item(self, item, spider):
        # Append the item to the list
        self.items.append(item)
        return item

    def close_spider(self, spider):
        # Convert the list of items to a DataFrame
        df = pd.DataFrame(self.items)

        # Display the DataFrame (you can also save to a file or other operations)
        print(df)
