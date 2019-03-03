# -*- coding: utf-8 -*-
#
#  ladr.py
#  Medalytik-Python
#
#  Created by Jorge Paravicini on 3/1/2019.
#  Copyright © 2018 Jorge Paravicini. All rights reserved.
#   

import json
import html
import re

import scrapy

from ..items import Job


class LADRSpider(scrapy.Spider):

    name = "ladr"
    website_name = "LADR"
    website_url = "https://ladr.de/karriere"
    data_url = "https://intermed-karriere.dvinci-hr.com/portal/LADR/jobPublication/list.json"

    def __init__(self, debug="1"):
        super(LADRSpider, self).__init__(self.name)

        self.debug = debug == "1"

    def start_requests(self):
        yield scrapy.Request(url=self.data_url, callback=self.parse, dont_filter=True)

    def parse(self, response):
        body = self.decode(response.text)
        json_response = json.loads(body)

        for json_object in json_response:
            yield self.parse_job(json_object)

    def parse_job(self, json_object):
        job = Job()
        job['in_development'] = self.debug
        job['website_name'] = self.website_name
        job['website_url'] = self.website_url
        job['url'] = self.strip_element('jobPublicationURL', json_object)
        job['title'] = self.strip_element('subtitle', json_object)
        job['area'] = self.strip_element('position', json_object)
        job['summary'] = self.strip_element('introduction', json_object)
        job['desc'] = self.strip_element('tasks', json_object)
        job['requirements'] = self.strip_element('profile', json_object)
        job['offer'] = self.strip_element('weOffer', json_object)
        region = next(iter(json_object['jobOpening']['locations']), None)
        if region is not None:
            job['regions'] = self.strip_element('name', region)

        job_opening = json_object['jobOpening']
        if job_opening is not None:
            job['schedule'] = self.strip_element('earliestEntryDate', job_opening)
            contract_period = job_opening['contractPeriod']
            if contract_period is not None:
                job['offering_type'] = self.strip_element('name', contract_period)
            org_unit = job_opening['orgUnit']
            if org_unit is not None:
                job['organization'] = self.strip_element('name', org_unit)

            contact = job_opening['responsibleUser']
            if contact is not None:
                first_name = self.strip_element('firstName', contact)
                last_name = self.strip_element('lastName', contact)
                job['contact_name'] = first_name + ' ' + last_name
                job['contact_field'] = self.strip_element('division', contact)
                job['contact_email'] = self.strip_element('email', contact)
                job['contact_phone'] = self.strip_element('telephone', contact)

        return job

    @staticmethod
    def strip_element(name, json_object):
        element = json_object[name]
        if isinstance(element, str):
            element = element.strip()
        return element

    @staticmethod
    def decode(string):
        string = html.unescape(string)
        regex = re.compile('<.*?>')
        return re.sub(regex, '', string)
