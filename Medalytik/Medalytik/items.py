#
#  items.py
#  Medalytik
#
#  Created by Jorge Paravicini on 10.06.18.
#  Copyright © 2018 Jorge Paravicini. All rights reserved.
#


import scrapy


class Job(scrapy.Item):
    in_development = scrapy.Field()
    about = scrapy.Field()
    area = scrapy.Field()
    date_availability = scrapy.Field()
    desc = scrapy.Field()
    summary = scrapy.Field()
    group = scrapy.Field()
    offer = scrapy.Field()
    requirements = scrapy.Field()
    environment = scrapy.Field()
    benefits = scrapy.Field()
    title = scrapy.Field()
    url = scrapy.Field()
    website_name = scrapy.Field()
    website_url = scrapy.Field()
    regions = scrapy.Field()
    queries = scrapy.Field()
    contact_name = scrapy.Field()
    contact_field = scrapy.Field()
    contact_phone = scrapy.Field()
    contact_email = scrapy.Field()
    info = scrapy.Field()
    info_phone = scrapy.Field()
    info_email = scrapy.Field()
    schedule = scrapy.Field()
    offering_type = scrapy.Field()
    organization = scrapy.Field()
