# -*- coding: utf-8 -*-
#
#  imdb_berlin.py
#  Medalytik-Python
#
#  Created by Jorge Paravicini on 2/22/2019.
#  Copyright © 2018 Jorge Paravicini. All rights reserved.
#

import scrapy
import scrapy.shell
from ..items import Job

url = "https://www.imd-berlin.de/imd-labor/allgemeines/stellenangebote.html"
domain = "https://www.imd-berlin.de/"


class IMDBBerlin(scrapy.Spider):
    name = 'IMDBBerlin'

    def __init__(self, debug="0"):
        super(IMDBBerlin, self).__init__(self.name)

        self.website_name = "IMDB Berlin"
        self.website_url = url

        self.debug = debug

    def start_requests(self):
        yield scrapy.Request(url, dont_filter=True)

    def parse(self, response):
        job_container_xpath = "//div[@class='ajjobs-job']"
        job_container = response.xpath(job_container_xpath)
        for job in job_container:
            link = job.xpath(".//a/@href").extract_first()
            uri = domain + link
            yield scrapy.Request(uri, callback=self.parse_job, dont_filter=True)

    def parse_job(self, response):
        job_element = response.xpath("//div[@class='tx-aj-jobs']")
        if job_element < 0:
            print("Failed to retrieve job element")
            return
        elif job_element > 1:
            print("Multiple job elements found")

        yield self.construct_job(job_element[0])

        scrapy.shell.inspect_response(response, self)

    def construct_job(self, job_element):
        job = Job()
        job['in_development'] = self.debug
        job['website_name'] = self.website_name
        job['website_url'] = self.website_url

        title = job_element.xpath("//h1").extract_first()
        if title:
            job['title'] = title.strip()

        region = job_element.xpath("//span[@class='pull-right']").extract_first()
        if region:
            job['region'] = region.strip()

        children = job_element.xpath("*")

        beginning = True
        for child in children:
            if child.xpath("name()").extract_first() == 'h5':
                continue
            if child.xpath("name()").extract_first() == 'h1':
                continue
            if child.xpath("@class").extract_first() == 'tx-aj-jobs':
                continue


