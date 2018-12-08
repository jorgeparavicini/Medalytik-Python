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

query_id_map = {
    "Administration/Direktion": "1146346",
    "Administration/Einkauf": "1140494",
    "Administration/Finanzen/Controlling": "1140495",
    "Administration/Marketing/Kommunikation": "1140496",
    "Administration/Projekte": "1140498",
    "Administration/Verwaltung/Assistenz": "1140499",
    "Administration/Human Resource Management": "1151355",
    "Ärztliches Personal/Abdomen-Stoffwechsel": "1140501",
    "Ärztliches Personal/Anästhesie": "1146347",
    "Ärztliches Personal/Diverse": "1140502",
    "Ärztliches Personal/Frau-Kind": "1146348",
    "Ärztliches Personal/Herz-Gefäss-Thorax": "1140503",
    "Ärztliches Personal/Innere Medizin-Onkologie": "1140504",
    "Ärztliches Personal/Intensivpflege": "1140505",
    "Ärztliches Personal/Neuro-Kopf": "1140506",
    "Ärztliches Personal/OPS": "1146349",
    "Ärztliches Personal/Trauma-Derma-Rheumatologie-Plastische Chirurgie und Notfallmedizin": "1140507",
    "Ärztliches Personal/Diagnostik": "1146352",
    "Bildung/Ausbildung & Weiterbildungen": "1140509",
    "Bildung/Lehrstellen": "1140510",
    "Bildung/Nachdiplomstudien (NDS)": "1140511",
    "Bildung/Praktika": "1140512",
    "Dipl. Pflegepersonal/Abdomen-Stoffwechsel": "1140514",
    "Dipl. Pflegepersonal/Anästhesie": "1140515",
    "Dipl. Pflegepersonal/Diverse": "1140156",
    "Dipl. Pflegepersonal/Frau-Kind": "1140517",
    "Dipl. Pflegepersonal/Herz-Gefäss-Thorax": "1140519",
    "Dipl. Pflegepersonal/Innere Medizin-Onkologie": "1140521",
    "Dipl. Pflegepersonal/Intensivpflege": "1140522",
    "Dipl. Pflegepersonal/Neuro-Kopf": "1140524",
    "Dipl. Pflegepersonal/OPS": "1140526",
    "Dipl. Pflegepersonal/Trauma– Derma– Rheuma– Plastische Chirurgie und Notfallmedizin": "1140527",
    "Dipl. Pflegepersonal/Diagnostik": "1146353",
    "FaGe/MPA/Fachfrau/mann Gesundheit": "1140529",
    "FaGe/MPA/Med. Praxisassistent/in": "1140530",
    "FaGe/MPA/Zentrale Sterilisationsversorgung": "1140531",
    "Forschung/Forschung": "1140533",
    "Gastronomie/Hotelleries/Hauswirtschaft/Gastronomie/Hotelleries/Hauswirtschaft": "1140535",
    "Med.-technisches & Med.-therapeutisches Personal/Biomedizinische Analytik": "1140537",
    "Med.-technisches & Med.-therapeutisches Personal/Diverse": "1140538",
    "Med.-technisches & Med.-therapeutisches Personal/Ergotherapie": "1146359",
    "Med.-technisches & Med.-therapeutisches Personal/Medizinisch-technische Radiologie": "1140539",
    "Med.-technisches & Med.-therapeutisches Personal/Physiotherapie": "1140540",
    "Soziale Berufe/Soziale Berufe": "1140542",
    "Technische Berufe/Informatik/Bau und Immobilien": "1140544",
    "Technische Berufe/Informatik/Informatik": "1140545",
    "Technische Berufe/Informatik/Technischer Dienst": "1140546"
}

id_query_name_map = {
    "1146346": "Direktion",
    "1140494": "Einkauf",
    "1140495": "Controlling",
    "1140496": "Marketing/Kommunikation",
    "1140498": "Projekte",
    "1140499": "Verwaltung/Assistenz",
    "1151355": "Human Resource Management",
    "1140501": "Abdomen-Stoffwechsel",
    "1146347": "Anästhesie",
    "1140502": "Diverse",
    "1146348": "Frau-Kind",
    "1140503": "Herz-Gefäss-Thorax",
    "1140504": "Innere Medizin-Onkologie",
    "1140505": "Intensivpflege",
    "1140506": "Neuro-Kopf",
    "1146349": "OPS",
    "1140507": "Trauma-Derma-Rheumatologie-Plastische Chirurgie und Notfallmedizin",
    "1146352": "Diagnostik",
    "1140509": "Ausbildung & Weiterbildungen",
    "1140510": "Lehrstellen",
    "1140511": "Nachdiplomstudien (NDS)",
    "1140512": "Praktika",
    "1140514": "Abdomen-Stoffwechsel",
    "1140515": "Anästhesie",
    "1140156": "Diverse",
    "1140517": "Frau-Kind",
    "1140519": "Herz-Gefäss-Thorax",
    "1140521": "Innere Medizin-Onkologie",
    "1140522": "Intensivpflege",
    "1140524": "Neuro-Kopf",
    "1140526": "OPS",
    "1140527": "Trauma– Derma– Rheuma– Plastische Chirurgie und Notfallmedizin",
    "1146353": "Diagnostik",
    "1140529": "Fachfrau/mann Gesundheit",
    "1140530": "Med. Praxisassistent/in",
    "1140531": "Zentrale Sterilisationsversorgung",
    "1140533": "Forschung",
    "1140535": "Gastronomie/Hotelleries/Hauswirtschaft",
    "1140537": "Biomedizinische Analytik",
    "1140538": "Diverse",
    "1146359": "Ergotherapie",
    "1140539": "Medizinisch-technische Radiologie",
    "1140540": "Physiotherapie",
    "1140542": "Soziale Berufe",
    "1140544": "Bau und Immobilien",
    "1140545": "Informatik",
    "1140546": "Technischer Dienst"
}


class USZSpider(scrapy.Spider):
    name = 'usz'

    def __init__(self, queries, debug="1"):
        super(USZSpider, self).__init__(self.name)

        self.url = 'http://jobs.usz.ch'

        self.website_name = 'Uni Spital Zürich'
        self.website_url = self.url
        self.regions = 'Zürich'

        self.debug = debug == "1"

        # Queries are passed as a list. Ex: ['Query1', 'Query2'].
        queries = queries.lstrip('[').rstrip(']').split(',')
        self.queries = []
        for query in queries:
            self.queries.append(query_id_map[query.strip()])

    def start_requests(self):
        for query in self.queries:
            yield scrapy.FormRequest(self.url,
                                     callback=self.parse,
                                     errback=error_back,
                                     formdata={'filter_10': query},
                                     dont_filter=True,
                                     meta={'query': query, 'offset': 0})

    def parse(self, response):
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
                                         formdata={'filter_10': response.meta['query'],
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
        job['in_development'] = self.debug
        job['website_name'] = self.website_name
        job['website_url'] = self.website_url
        job['queries'] = [id_query_name_map[query]]
        job['regions'] = self.regions

        # Make a null check before stripping the string, to prevent strip on none, type errors.
        title_container = job_container.xpath('div[1]/a/text()').extract_first()
        if title_container:
            job['title'] = title_container.strip()

        group_container = job_container.xpath('div[2]/a/text()').extract_first()
        if group_container:
            group_container = group_container.strip()
            if group_container.startswith("Berufsgruppe:"):
                group_container = group_container[13:]
            job['group'] = group_container.strip()

        area_container = job_container.xpath('div[3]/a/text()').extract_first()
        if area_container:
            area_container = area_container.strip()
            if area_container.startswith("Bereich:"):
                area_container = area_container[8:]
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
        job['benefits'] = "- " + "\n- ".join(benefits)
        return job
