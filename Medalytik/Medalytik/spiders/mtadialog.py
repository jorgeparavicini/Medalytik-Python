# -*- coding: utf-8 -*-
#
#  mtadialog.py
#  Medalytik-Python
#
#  Created by Jorge Paravicini on 12/6/2018.
#  Copyright © 2018 Jorge Paravicini. All rights reserved.
#
#
#

import scrapy

from ..items import Job


class MTADialogSpider(scrapy.Spider):
    """
    This spider scrapes the website MTA Dialog.

    The search is split up into queries, each separated by a ','.
    The queries represent each an input in the provided search field on the website.

    One job can appear on multiple query searches. To prevent duplicate jobs to appear in the database,
    they get filtered, and the queries are joined, in the pipeline.

    MTA Dialog uses cookies to store session information.

    To set multiple search parameters, we need to call the 'url' with the wanted parameters on a session.
    To make sure that the parameters are saved, upon sending a scrapy request set the 'cookiejar' in the meta data to
    the correct cookie session.

    Only one parameter is allowed to be passed at a time, to get multiple parameters, the search is additive.
    Meaning, once a search url has been requested, the search parameters will be stored in that 'cookiejar'

    The last call will contain the first page of the queried results.
    To get further pages, make sure there is a next page by passing the response to the 'has_next_page'.
    If that returns true, the next page can be called by sending a request with the 'next_url'
    Just make sure to add the current cookie context.

    To invoke this spider, from inside the Medalytik project run the following command.

        scrapy crawl mta_dialog -a queries="YOUR_QUERIES_SEPARATED_BY_A_COMMA"
    """
    # The name of the spider, needed to be invoked.
    name: str = 'mta_dialog'
    # The default path to the job offers website.
    url: str = 'https://www.mta-dialog.de/stellenmarkt.html'

    # The display name of the website.
    website_name: str = "MTA Dialog"
    # The home url to the website.
    website_url: str = 'https://www.mta-dialog.de'

    @property
    def next_url(self) -> str:
        """
        The url pointing to the next page.

        Request this url, when all search parameters have been set,
         the first page been scraped and you are sure there is a next page.
        If this url is requested and there is no next page, the response will be invalid, and unable to be read by this
        spider.
        :return: The next page url.
        """
        return 'https://www.mta-dialog.de/stellenmarkt.html?tx_jobs_pi1[action]=next'

    def __init__(self, queries: str="", debug: str="1"):
        """
        Initialize a new spider to crawl MTA Dialog.

        Do not instantiate this class on your own, use the scrapy command interface to do so.
        (See docstring of spider for further information)

        The queries parameter, should be a string object that separates multiple queries with a comma.
        Each query will generate its own cookie session, this way we can filter the different jobs.

        :param queries: Queries separated by a comma to be searched.
        :param debug: Should the result be stored in the debug, or release database.
        """
        super(MTADialogSpider, self).__init__(self.name)

        self.debug = debug == "1"
        self.queries = queries.split(',')

    @staticmethod
    def query_url(query: str) -> str:
        """
        Creates an url with the specified query. When requested, the result will be stored in the cookie jar.
        Make sure not to reset cookies.
        For parsing the information, make sure to stay in the same cookie session.
        :param query: The query to search for.
        :return: The url with embodied query.
        """
        base_url = "https://www.mta-dialog.de/stellenmarkt.html?tx_jobs_pi1%5Baction%5D=fullTextSearch&" \
                   "tx_jobs_pi1[value]="
        return base_url + query

    def start_requests(self) -> scrapy.http.Request:
        """
        Starts the scraper.

        Here we initiate all sessions.
        One session is created for each query. The session data are stored inside the 'cookiejar' metadata.

        * THIS IS A GENERATOR *

        :return: All session requests. They are passed to the scrapy framework to be processed.
        """
        for i, query in enumerate(self.queries):
            url = self.query_url(query)
            yield scrapy.Request(url,
                                 meta={'cookiejar': i,
                                       'queries': [query]},
                                 dont_filter=True)

    def parse(self, response: scrapy.http.Response):
        """
        After the request has been processed by the scrapy framework, the response will be passed here.

        First we yield all job items from the current response html body.
        Afterwards if there is a next page, we request the next page
        making sure to use the same session from the cookiejar.

        * THIS IS A GENERATOR *

        :param response: The response of the scrapy request.
        """
        for job in self.parse_jobs(response):
            yield job

        if self.has_next_page(response):
            yield scrapy.Request(self.next_url,
                                 meta={'cookiejar': response.meta['cookiejar'],
                                       'queries': response.meta['queries']},
                                 dont_filter=True)

    @staticmethod
    def has_next_page(response: scrapy.http.Response):
        """
        Checks whether the response object contains the button for the next page.
        We identify the next object by a class named: "glyphControl glyphPaginationNext"

        If this element is present in the response html body, we know there is a next page.

        :param response: The response of the scrapy request.
        :return: A boolean value indicating whether there is a next page.
        """
        next_buttons = response.xpath('//span[@class="glyphControl glyphPaginationNext"]').extract()
        return bool(next_buttons)

    def parse_jobs(self, response: scrapy.http.Response):
        """
        Parses all jobs in the current response html body.

        Each job is inside a class named "jobHit"
        We can enumerate over all these classes,
        and pass this container to the parse job function which will parse one container.

        These parsed job containers will be yielded back.

        * THIS IS A GENERATOR *

        :param response: The response of the scrapy request.
        :return: All parsed jobs from the response body.
        """
        hits = response.xpath('//div[@class="jobHit"]')
        for hit in hits:
            job = self.default_job()
            job['queries'] = response.meta['queries']
            for i in MTADialogSpider.parse_job(hit, job):
                yield i

    def default_job(self):
        """
        Initializes a job with the common elements.

        The common elements from this website are:
            - In Development
            - Website Name
            - Website URL

        :return: The job element initialized with default values concerning this website.
        """
        job = Job()

        job['in_development'] = self.debug
        job['website_name'] = self.website_name
        job['website_url'] = self.website_url

        return job

    @staticmethod
    def parse_job(content: scrapy.selector.Selector, job: Job):
        """
        Parses a job container from a scrapy response.

        It will call the parse job function, to see if the detail scraping for this organization has been implemented.

        :param content: The selector pointing to the job to parse.
        :param job: The pre-initialized job. Needs to be filled with the query.
        :return: The filled out job element.
        """

        job['title'] = content.xpath('.//div[@class="hidden-xs likeH2"]/text()').extract_first().strip()
        job['organization'] = content.xpath('.//div[@class="hidden-xs likeH3"]/text()').extract_first().strip()
        column1_elements = content.xpath('.//div[@class="jobHitColumn1"]/p')
        job['regions'] = column1_elements[0].xpath('text()').extract_first().strip()
        job['area'] = ' '.join(column1_elements[1].xpath('text()').extract_first().strip().split())
        job['url'] = 'https://www.mta-dialog.de/' + \
                     content.xpath('.//a[@class="detailLinkParent"]/@href').extract_first().strip()
        return MTADialogSpider.parse_job_details(job)

    @staticmethod
    def parse_job_details(job: Job):
        """
        Checks whether the detail parser for the organization of the passed job has been implemented.
        If so return the job with the filled out details.
        Otherwise return the passed job.

        :param job: The job to scrape the details for.
        :return: The fully filled job item.
        """
        if job['organization'].casefold() == "ORGENTEC Diagnostika GmbH".casefold():
            yield scrapy.Request(job['url'], callback=MTADialogSpider.parse_orgentec, meta={'job': job},
                                 dont_filter=True)
        else:
            yield job

    @staticmethod
    def parse_orgentec(response: scrapy.http.Response):
        """
        Parses the details for the orgentec organization.

        :param response: The response body, of the requested job that should be filled.
        :return: The fully filled orgentec job.
        """
        job = response.meta['job']

        yield job
