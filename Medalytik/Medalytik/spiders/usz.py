# -*- coding: utf-8 -*-
#
#  usz.py
#  Medalytik
#
#  Created by Jorge Paravicini on 10.06.18.
#  Copyright © 2018 Jorge Paravicini. All rights reserved.
#


import re

import scrapy

from ..items import JobItem
from . import error_back


class USZSpider(scrapy.Spider):
    name = 'usz'

    def __init__(self, queries):
        super(USZSpider, self).__init__(self.name)

        self.url = 'http://jobs.usz.ch'

        self.website_name = 'Uni Spital Zürich'
        self.website_url = self.url
        self.regions = ['Zürich']

        # Always save the status. So we know if the website worked.
        self.saved_status = False
        # Queries are passed as a list. Ex: ['Query1', 'Query2'].
        self.queries = queries.lstrip('[').rstrip(']').split(',')
        for i, query in enumerate(self.queries):
            self.queries[i] = query.strip()

        self.parsed_jobs = []

    def start_requests(self):
        for query in self.queries:
            yield scrapy.FormRequest(self.url,
                                     callback=self.parse,
                                     errback=error_back,
                                     formdata={'query': query},
                                     dont_filter=True,
                                     meta={'query': query, 'offset': 0})

    def parse(self, response):
        self.save_status()

        job_container_xpath = '//*[@id="itemlist"]/div[1]/div'
        job_container_element = response.xpath(job_container_xpath)
        for job_container in job_container_element:
            for element in self.parse_job_container(job_container, response.meta['query']):
                yield element

        for element in self.load_next_page(response):
            yield element

    def load_next_page(self, response):
        # Check if there is a next website. The btn-forward's class is 'disableClick' if it's the last page.
        next_page_element = response.xpath('//*[@id="btn-forward"]')
        try:
            first_next_page_element = next_page_element[0]
            next_page_class_element = first_next_page_element.xpath('@class')
            next_page_class = next_page_class_element[0].extract()

            # Load the next page if the next page button is active.
            if next_page_class != 'disableClick':
                offset = response.meta['offset'] + 10
                yield scrapy.FormRequest(self.url,
                                         formdata={'query': response.meta['query'],
                                                   'offset': offset,
                                                   },
                                         meta={'query': response.meta['query'], 'offset': offset},
                                         callback=self.parse,
                                         dont_filter=True,
                                         errback=error_back
                                         )

        except IndexError:
            # In case the extraction of the next page button fails, we assume there is no next page.
            return

    def parse_job_container(self, job_container, query):
        job = JobItem()
        job['website_name'] = self.website_name
        job['website_url'] = self.website_url
        job['queries'] = [query]
        job['regions'] = self.regions

        # Make a null check before stripping the string, to prevent strip on none type errors.
        title_container = job_container.xpath('div[1]/a/text()').extract_first()
        if title_container:
            job['title'] = title_container.strip()

        group_container = job_container.xpath('div[2]/a/text()').extract_first()
        if group_container:
            job['group'] = group_container.strip()

        area_container = job_container.xpath('div[3]/a/text()').extract_first()
        if area_container:
            job['area'] = area_container.strip()

        summary_container = job_container.xpath('a/div/text()').extract_first()
        if summary_container:
            job['summary'] = summary_container.strip()

        link = job_container.xpath('div[1]/a/@href').extract_first()

        yield scrapy.Request(url=link, callback=self.parse_job_website, dont_filter=True, meta={'job': job})

    def parse_job_website(self, response):
        job = response.meta['job']
        job['url'] = response.request.url
        job = self.parse_date_availability(response, job)
        job = self.parse_description(response, job)
        job = self.parse_requirements(response, job)
        job = self.parse_contact(response, job)
        job = self.parse_info(response, job)
        job = self.parse_offer(response, job)
        job = self.parse_environment(response, job)
        job = self.parse_benefits(response, job)

        yield job

    @staticmethod
    def parse_date_availability(response, job):
        date_availability = response.xpath('//*[@id="vereinbarung"]/text()').extract_first()
        if date_availability:
            job['date_availability'] = date_availability.strip()
        return job

    @staticmethod
    def parse_description(response, job):
        description_content = response.xpath('//*[@id="job-description"]/div/div[1]')
        description_list = description_content.xpath('ul/li')
        description = ''
        if len(description_list):
            for description_element in description_list:
                extracted_text = description_element.xpath('string()').extract_first()
                if extracted_text:
                    description += '\n- '
                    description += extracted_text.strip()
        else:
            description_list = response.xpath("//div[@class='group']/div[@class='span_1_of_2']/text() | "
                                              "//div[@class='group']/div[@class='span_1_of_2']/br")
            for item in description_list:
                if type(item.root) is str:
                    description += item.root
                else:
                    description += '\n'
            description = '-' + re.sub(r'\n+', '\n-', description.strip())
        job['desc'] = description.strip()
        return job

    @staticmethod
    def parse_requirements(response, job):
        requirements_content = response.xpath("//div[@class='group']/div[@class='span_2_of_2']")
        requirements_list = requirements_content.xpath("ul/li")
        requirements = ""
        if len(requirements_list) > 0:
            for i in requirements_list:
                requirements += "\n- "
                requirements += i.xpath("string()").extract_first()
        else:
            requirements_list = response.xpath("//div[@class='group']/div[@class='span_2_of_2']/text() | "
                                               "//div[@class='group']/div[@class='span_2_of_2']/br")
            for item in requirements_list:
                if type(item.root) is str:
                    requirements += item.root
                else:
                    requirements += '\n'
            requirements = '-' + re.sub(r'\n+', '\n-', requirements.strip())
        job['requirements'] = requirements.strip()
        return job

    @staticmethod
    def parse_contact(response, job):
        job = USZSpider.parse_contact_name(response, job)
        return job

    @staticmethod
    def parse_contact_name(response, job):
        contact_list = response.xpath('//*[@id="contact-box"]//div[@class="contact_span_2_of_2"]/text()')
        if len(contact_list) == 3:
            contact = contact_list[0].extract()
            field = contact_list[1].extract()
        else:
            text = contact_list[0].extract()
            if text:
                text = text.strip()
            fields = text.split(',')
            try:
                contact = fields[1]
            except IndexError:
                contact = 'Failed to retrieve.'
            try:
                field = fields[2]
            except IndexError:
                field = 'Failed to retrieve'

        if contact:
            job['contact_name'] = contact.strip()
        if field:
            job['contact_field'] = field.strip()

        return job

    @staticmethod
    def parse_info(response, job):
        job = USZSpider.parse_main_info(response, job)
        job = USZSpider.parse_info_mail(response, job)
        job = USZSpider.parse_phone(response, job)
        return job

    @staticmethod
    def parse_main_info(response, job):
        info = response.xpath('//*[@id="contact-box"]/div[1]/text()')[1].extract()
        if info:
            job['info'] = info.strip()
        return job

    @staticmethod
    def parse_info_mail(response, job):
        replaced_body = response.replace(body=response.body.replace(b'<br>', b'\n'))
        contact_list = replaced_body.xpath("//div[@class='contact_span_2_of_2']/text()")
        contact = ""
        for i in contact_list:
            if type(i.root) is str:
                contact += i.root
            else:
                contact += "\n"
        email = next(iter(re.findall(r"[\w.-]+@[\w.-]+", contact.strip())), None)
        if email is None:
            email = response.xpath("//div[@id='phone-box']/a[@title='E-Mail']/text()").extract_first()

        if email is not None:
            email = email.strip()
        job['info_email'] = email
        return job

    @staticmethod
    def parse_phone(response, job):
        phone = response.xpath("//div[@id='phone-box']/img[@class='icon-phone']") \
            .xpath('..').xpath('string()').extract_first()
        if phone is not None:
            phone = phone.strip()
        job['info_phone'] = phone
        return job

    @staticmethod
    def parse_offer(response, job):
        offer = response.xpath('//*[@id="angebot"]/text()')[1].extract()
        if offer:
            job['offer'] = offer.strip()
        return job

    @staticmethod
    def parse_environment(response, job):
        environment = response.xpath('//div[@id="ueber-usz"]/text()')[1].extract()
        if environment:
            job['environment'] = environment.strip()
        return job

    @staticmethod
    def parse_benefits(response, job):
        benefits_list = response.xpath('//div[@class="benefit-text"]')
        benefits = []
        for benefit_container in benefits_list:
            benefit = benefit_container.xpath('text()').extract()
            if benefit:
                benefit_string = "".join(benefit).strip()
                benefits.append(benefit_string)
                print(benefit_string)
        job['benefits'] = benefits
        return job

    def save_status(self):
        if not self.saved_status:
            self.saved_status = True
            yield {'status': 200}
