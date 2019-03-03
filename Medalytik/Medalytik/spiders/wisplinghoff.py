# -*- coding: utf-8 -*-
#
#  wisplinghoff.py
#  Medalytik-Python
#
#  Created by Jorge Paravicini on 3/2/2019.
#  Copyright © 2018 Jorge Paravicini. All rights reserved.
#

import warnings

import scrapy
import scrapy.shell

from ..items import Job


class WisplinghoffSpider(scrapy.Spider):

    name = "wisplinghoff"
    website_name = "Wisplinghoff"
    website_url = "https://www.wisplinghoff.de/das-labor/job-karriere/"
    domain = "https://www.wisplinghoff.de/"

    def __init__(self, debug="1"):
        super(WisplinghoffSpider, self).__init__(self.name)
        self.debug = debug == "1"

    def start_requests(self):
        yield scrapy.Request(url=self.website_url, callback=self.parse, dont_filter=True)

    def parse(self, response):
        jobs = response.xpath("//article[@class='news-item']")
        for container in jobs:
            for job in self.parse_job_container(container):
                yield job

    def parse_job_container(self, container):
        text = container.xpath("./div[@class='text']")
        info_string = text.xpath("string(./div[@class='news-info'])").extract_first()

        job = Job()

        job['title'] = text.xpath('string(./h2)').extract_first().strip()
        job['website_name'] = self.website_name
        job['website_url'] = self.website_url
        job['in_development'] = self.debug

        if isinstance(info_string, str):
            info_list = info_string.split('|')
            if len(info_list) == 2:
                job['date_availability'] = info_list[0].strip()
                job['regions'] = info_list[1].strip()
            else:
                warnings.warn("News info does not contain 2 elements.")
        else:
            warnings.warn("News info element does not exist.")

        link = self.domain + text.xpath("p/a/@href").extract_first()

        if isinstance(link, str):
            link = link.strip()
            job['url'] = link
            yield scrapy.Request(url=link, callback=self.parse_article, dont_filter=True, meta={"job": job})

    def parse_article(self, response):
        job = response.meta['job']
        print(job)
        #return job
