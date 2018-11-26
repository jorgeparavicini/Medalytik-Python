import scrapy
import re
from string import digits

from ..items import JobItem


class DiagnosticumSpider(scrapy.Spider):
    name = 'diagnosticum'

    def __init__(self):
        super(DiagnosticumSpider, self).__init__(self.name)

        self.url = "https://www.diagnosticum.eu/diagnosticum/stellenangebote"
        self.website_name = "Diagnosticum"
        self.website_url = self.url

    def start_requests(self):
        yield scrapy.Request(self.url,
                             callback=self.parse,
                             dont_filter=True)

    def parse(self, response):
        container_xpath = "//div[@id='accordion_ba33b4dd747ef380d858b17b6862cdb6']/child::*"
        job_element_list = response.xpath(container_xpath)
        for i in range(0, len(job_element_list), 2):
            if len(job_element_list[i + 1].xpath('div/child::*')) < 10:
                continue
            yield self.parse_job(job_element_list[i], job_element_list[i + 1])

    def parse_job(self, header_element, body_element):
        job = JobItem()
        job['in_development'] = False
        job['queries'] = ["Default"]
        job['website_name'] = self.website_name
        job['website_url'] = self.website_url
        job = self.parse_header(header_element, job)
        job = self.parse_body(body_element, job)
        return job

    @staticmethod
    def parse_header(header_element, job):
        title = header_element.xpath("string()").extract_first().strip()
        job['title'] = title
        return job

    @staticmethod
    def parse_body(body_element, job):
        body_elements = body_element.xpath('div/child::*')

        summary_finished = False
        summary = ""

        for i, selector in enumerate(body_elements):
            if selector.xpath('li') or not selector.xpath('string()').extract_first():
                continue
            if selector.xpath('strong'):
                summary_finished = True
                section = selector.xpath('string()').extract_first()
                if section == "Ihre Aufgaben" or section == "Ihre Aufgaben:":
                    job['desc'] = '- ' + '\n- '.join(body_elements[i + 1].xpath("li/text()").extract()).strip()
                elif section == "Ihr Profil" or section == "Ihr Profil:":
                    job['requirements'] = '- ' + '\n- '.join(body_elements[i + 1].xpath("li/text()").extract()).strip()
                elif section == "Unser Angebot" or section == "Ihre Chance:" or section == "Ihre Chance":
                    job['offer'] = '- ' + '\n- '.join(body_elements[i + 1].xpath("li/text()").extract()).strip()
                elif section.startswith("Ihre Bewerbung"):
                    pass
                elif '@' in section:
                    job['contact_email'] = re.search(r'[\w\.-]+@[\w\.-]+', section).group(0)
                elif section.startswith('Diagnosticum'):
                    postal_re = re.compile(r'[0-9]{3,}')
                    text_list = selector.xpath("text()").extract()
                    for text in text_list:
                        if postal_re.match(text):
                            job['regions'] = [text.lstrip(digits).strip()]
                else:
                    job['area'] = section

            else:
                if not summary_finished:
                    summary += " " + selector.xpath("string()").extract_first()
                else:
                    text_list = selector.xpath("text()").extract()
                    postal_re = re.compile(r'[0-9]{3,}')
                    tel_re = re.compile(r'^Tel\. \+')
                    for text in text_list:
                        if postal_re.match(text):
                            job['regions'] = [text.lstrip(digits).strip()]
                        elif tel_re.match(text):
                            job['contact_phone'] = text.split(u'\xa0', 1)[0]

        job['summary'] = summary

        return job

