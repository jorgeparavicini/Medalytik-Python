# -*- coding: utf-8 -*-
#
#  synlab.py
#  Medalytik-Python
#
#  Created by Jorge Paravicini on 11/29/2018.
#  Copyright © 2018 Jorge Paravicini. All rights reserved.
#

import re
import json

import scrapy
import scrapy.shell

from ..items import Job


class SynlabSpider(scrapy.Spider):
    name = "synlab"

    url = "https://api-synlab.beesite.de/search/"
    website_name = "Synlab"
    website_url = "https://www.karriere-synlab.de"

    MAX_RESPONSE_SIZE = 2147483647

    def __init__(self, queries="", debug="1"):
        super(SynlabSpider, self).__init__(self.name)

        self.queries = queries.split(',')
        self.debug = debug == "1"

    def url_from(self, query):
        """
        Creates a request url with the form data integrated.
        :param query: The job areas to search.
        :return: The generated url with integrated form data.
        """
        url = self.url \
              + "?data={" \
              + "\"SearchParameters\":{\"FirstItem\":1,\"CountItem\":" + str(self.MAX_RESPONSE_SIZE) + "}," \
              + "\"SearchCriteria\":["

        if query is not None:
            url += ("{\"CriterionName\":\"PositionFormattedDescription.Content\",\"CriterionValue\":[\"" + query
                    + "\"]}")

        url += "]}"
        return url

    def start_requests(self):
        for query in self.queries:
            yield scrapy.Request(url=self.url_from(query),
                                 callback=self.parse,
                                 dont_filter=True,
                                 meta={'query': query}
                                 )

    def parse(self, response):
        json_response = json.loads(response.body_as_unicode())
        response_items = json_response['SearchResult']['SearchResultItems']
        for response_item in response_items:
            job = self.init_job_item()
            response_item = response_item['MatchedObjectDescriptor']
            job['queries'] = [response.meta['query']]
            for element in self.parse_job(response_item, job):
                yield element

    def init_job_item(self):
        job = Job()
        job['in_development'] = self.debug
        job['website_name'] = self.website_name
        job['website_url'] = self.website_url

        return job

    def parse_job(self, job_dict, job):
        job['url'] = job_dict['PositionURI']
        job['title'] = job_dict['PositionTitle']
        schedule = next(iter(job_dict.get('PositionSchedule') or []), None)
        if schedule is not None:
            job['schedule'] = schedule['Name']
        offering = next(iter(job_dict.get('PositionOfferingType') or []), None)
        if offering is not None:
            job['offering_type'] = offering['Name']
        location = next(iter(job_dict.get('PositionLocation') or []), None)
        if location is not None:
            job['regions'] = location['CityName']

        yield scrapy.Request(job['url'],
                             callback=self.parse_job_uri,
                             dont_filter=True,
                             meta={'job': job}
                             )

    def parse_job_uri(self, response):
        job = response.meta['job']

        job = self.parse_base_info(response, job)
        job = self.parse_environment(response, job)
        job = self.parse_description(response, job)
        job = self.parse_requirements(response, job)
        job = self.parse_offer(response, job)
        job = self.parse_contact(response, job)

        yield job

    @staticmethod
    def parse_base_info(response, job):
        base_info = response.xpath('//section[@class="base-info row"]/*')
        # Availability is the third item of the base info row.
        job['date_availability'] = SynlabSpider.get_base_info(base_info, "Zeitpunkt")

        # Area is the fifth item of the base info row.
        job['area'] = SynlabSpider.get_base_info(base_info, "Tätigkeitsfeld")
        return job

    @staticmethod
    def get_base_info(base_info, name):
        for info in base_info:
            if info.xpath('.//strong/text()').extract_first().strip().rstrip(':') == name:
                return info.xpath('text()')[1].extract().strip()

    @staticmethod
    def parse_environment(response, job):
        job['environment'] = response.xpath('string(//section[@itemprop="description"])').extract_first().strip()
        return job

    @staticmethod
    def job_divisions(response):
        return response.xpath('//div[@id="jobad"]//div[@class="col-sm-12"]')

    @staticmethod
    def get_division(response, name):
        divisions = SynlabSpider.job_divisions(response)
        for division in divisions:
            path = division.xpath('string(.//div[@class="panel-heading"])')
            title = path.extract_first().strip().rstrip(':')
            if title.casefold() == name.casefold():
                return division

    @staticmethod
    def get_division_from_names(response, names):
        for name in names:
            division = SynlabSpider.get_division(response, name)
            if division is not None:
                return division

    @staticmethod
    def parse_description(response, job):
        division = SynlabSpider.get_division_from_names(response, [
            "Das sind Ihre Aufgaben"
        ])
        if division is not None:
            job['desc'] = division.xpath('string(.//div[@class="panel-body"])').extract_first().strip()
        return job

    @staticmethod
    def parse_requirements(response, job):
        division = SynlabSpider.get_division_from_names(response, [
            "Das bringen Sie mit"
        ])
        if division is not None:
            job['requirements'] = division.xpath('string(.//div[@class="panel-body"])').extract_first().strip()
        return job

    @staticmethod
    def parse_offer(response, job):
        division = SynlabSpider.get_division_from_names(response, [
            "Das können Sie von uns erwarten"
        ])
        if division is not None:
            job['offer'] = division.xpath('string(.//div[@class="panel-body"])').extract_first().strip()
        return job

    @staticmethod
    def parse_contact(response, job):
        division = SynlabSpider.get_division_from_names(response, [
            "Kontakt & Bewerbung"
        ])
        if division is None:
            return job

        text = division.xpath('string(.//div[@class="panel-body"])').extract_first()
        split_text = text.split()
        for i, word in enumerate(split_text):
            if (word == "Herr" or word == "Frau") and i < len(split_text - 1):
                job['contact_name'] = word + split_text[i+1]

        result = re.search(r'[^@\s]+@[^@\s]+\.[^@\s]+', text)
        if result is not None:
            job['contact_email'] = result.group()
        tel_groups = text.split('Tel.:')
        if len(tel_groups) > 1:
            job['contact_phone'] = tel_groups[1]
        return job
