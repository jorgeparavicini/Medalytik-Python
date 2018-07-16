# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import datetime
import pymongo
from . import constants

time_format = '%d %m %Y'


class MongoDBPipeline(object):
    """
    Connects to a Mongo Database and writes all the jobs in json like format to it.
    To Change the connection to the database change the 'MONGO URI' and 'MONGO DB' values from the settings.
    Also change the collection value below.
    """

    COLLECTION_NAME = 'Medalytik'
    MONGO_URI_SETTINGS_NAME = 'MONGO_URI'
    MONGO_DATABASE_SETTINGS_NAME = 'MONGO_DB'

    # -- DATABASE NAMES --

    DB_ABOUT = 'about'
    DB_AREA = 'area'
    DB_BENEFITS = 'benefits'
    DB_CONTACT_ID = 'contact_id'
    DB_CONTACT_NAME = 'name'
    DB_CONTACT_PHONE = 'phone'
    DB_CONTACT_FIELD = 'field'
    DB_CONTACT_MAIL = 'email'
    DB_DATE_AVAILABILITY = 'date_available'
    DB_DESCRIPTION = 'desc'
    DB_ENVIRONMENT = 'environment'
    DB_GROUP = 'group'
    DB_ID = '_id'
    DB_INFO_ID = 'info_id'
    DB_INFO = 'info'
    DB_INFO_PHONE = 'phone'
    DB_INFO_MAIL = 'email'
    DB_INFO_LAST_UPDATED = 'last_updated'
    DB_LAST_UPDATED = 'last_updated'
    DB_OFFER = 'offer'
    DB_QUERY_IDS = 'query_ids'
    DB_QUERY_NAME = 'name'
    DB_QUERY_LAST_UPDATED = 'last_updated'
    DB_REGION_IDS = 'region_ids'
    DB_REGION_NAME = 'name'
    DB_REGION_LAST_UPDATED = 'last_updated'
    DB_REQUIREMENTS = 'requirements'
    DB_SUMMARY = 'summary'
    DB_TITLE = 'title'
    DB_URL = 'url'
    DB_WEBSITE_ID = 'website_id'
    DB_WEBSITE_NAME = 'name'
    DB_WEBSITE_URL = 'url'
    DB_WEBSITE_LAST_UPDATED = 'last_updated'

    # -- ITEM NAMES --

    JOB_ABOUT = 'about'
    JOB_AREA = 'area'
    JOB_BENEFITS = 'benefits'
    JOB_CONTACT_ID = 'contact_id'
    JOB_CONTACT_NAME = 'contact_name'
    JOB_CONTACT_PHONE = 'contact_phone'
    JOB_CONTACT_FIELD = 'contact_field'
    JOB_CONTACT_MAIL = 'contact_email'
    JOB_DATE_AVAILABILITY = 'date_availability'
    JOB_DESCRIPTION = 'desc'
    JOB_ENVIRONMENT = 'environment'
    JOB_GROUP = 'group'
    JOB_INFO_ID = 'info_id'
    JOB_INFO = 'info'
    JOB_INFO_PHONE = 'info_phone'
    JOB_INFO_MAIL = 'info_email'
    JOB_LAST_UPDATED = 'last_updated'
    JOB_OFFER = 'offer'
    JOB_QUERIES = 'queries'
    JOB_REGIONS = 'regions'
    JOB_REQUIREMENTS = 'requirements'
    JOB_SUMMARY = 'summary'
    JOB_TITLE = 'title'
    JOB_URL = 'url'
    JOB_WEBSITE_ID = 'website_id'
    JOB_WEBSITE_NAME = 'website_name'
    JOB_WEBSITE_URL = 'website_url'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.client = None
        self.db = None

    @classmethod
    def from_crawler(cls, crawler):
        """Initiate the URI and database strings."""
        return cls(
            mongo_uri=constants.MONGO_URI,
            mongo_db=crawler.settings.get(MongoDBPipeline.MONGO_DATABASE_SETTINGS_NAME)
        )

    def open_spider(self, _):
        """Connect to the Mongo Database."""
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, _):
        """Close the connection to prevent memory leaks."""
        self.client.close()

    def process_item(self, item, _):
        """
        Adds all the objects as documents {dictionary like json} to the database.
        Since the MongoDB is not RDBMS we create it ourselves.

        First we handle all the objects, website, regions, queries, contact and info.
        Then add the id of each object to the job document.
        """
        website_id = self.handle_website(item)
        region_ids = self.handle_regions(item)
        query_ids = self.handle_queries(item)
        contact_id = self.handle_contact(item)
        info_id = self.handle_info(item)

        today = datetime.now().strftime(time_format)

        # Leave the query setting, since it is not complete.
        # A job can have multiple queries, but only one query is passed per item.
        # So me have to make sure the queries are up to date each day.
        job_dict = {
            self.DB_AREA: item.get(self.JOB_ABOUT),
            self.DB_ABOUT: item.get(self.JOB_AREA),
            self.DB_BENEFITS: item.get(self.JOB_BENEFITS),
            self.DB_CONTACT_ID: contact_id,
            self.DB_DATE_AVAILABILITY: item.get(self.JOB_DATE_AVAILABILITY),
            self.DB_DESCRIPTION: item.get(self.JOB_DESCRIPTION),
            self.DB_ENVIRONMENT: item.get(self.JOB_ENVIRONMENT),
            self.DB_GROUP: item.get(self.JOB_GROUP),
            self.DB_INFO_ID: info_id,
            self.DB_LAST_UPDATED: today,
            self.DB_OFFER: item.get(self.JOB_OFFER),
            self.DB_REGION_IDS: region_ids,
            self.DB_REQUIREMENTS: item.get(self.JOB_REQUIREMENTS),
            self.DB_SUMMARY: item.get(self.JOB_SUMMARY),
            self.DB_TITLE: item.get(self.JOB_TITLE),
            self.DB_URL: item.get(self.JOB_URL),
            self.DB_WEBSITE_ID: website_id,
        }

        existing_job = self.db.Jobs.find_one(
            {self.DB_WEBSITE_ID: website_id,
             self.DB_TITLE: item[self.JOB_TITLE]}
        )

        if existing_job:
            # Reset the queries each day. As the search terms may have changed.
            if existing_job[self.DB_LAST_UPDATED] != today:
                self.db.Jobs.update(
                    {self.DB_ID: existing_job[self.DB_ID]},
                    {'$set': {self.DB_QUERY_IDS: []}}
                )

            # Make sure to add the queries which are not present yet in the document.
            for query in query_ids:
                if query not in existing_job[self.DB_QUERY_IDS]:
                    new_queries = existing_job[self.DB_QUERY_IDS]
                    new_queries.append(query)
                    self.db.Jobs.update(
                        {self.DB_ID: existing_job[self.DB_ID]},
                        {'$set': {self.DB_QUERY_IDS: new_queries}}
                    )

            # Now we need to update the rest of the job just in case something would have changed.
            self.db.Jobs.update(
                {self.DB_ID: existing_job[self.DB_ID]},
                {'$set': job_dict}
            )
        else:
            # When we create a job we can safely add the queries,
            # as we can't perform any checks whether the queries already exist.
            job_dict[self.DB_QUERY_IDS] = query_ids
            self.db.Jobs.insert(job_dict)
        return item

    def handle_website(self, item):
        """
        Creates the website document for the item passed.
        If the document already exists, the 'last updated' value will be updated to today.
        :return: The id of the website for the job.
        """
        existing_website = self.db.Websites.find_one(
            {self.DB_WEBSITE_NAME: item.get(self.JOB_WEBSITE_NAME)}
        )

        today = datetime.now().strftime(time_format)

        if existing_website:
            # Just make sure there are no weird websites in the database.
            assert existing_website[self.DB_WEBSITE_URL] == item.get(self.JOB_WEBSITE_URL),\
                'Incorrect website url passed for website: {0}. URL: {1}'.format(item[self.JOB_WEBSITE_NAME],
                                                                                 item[self.JOB_WEBSITE_URL])
            # Update last updated time.
            if existing_website[self.DB_WEBSITE_LAST_UPDATED] != today:
                self.db.Websites.update(
                    {self.DB_ID: existing_website[self.DB_ID]},
                    {'$set': {self.DB_WEBSITE_LAST_UPDATED: today}}
                )

            return existing_website[self.DB_ID]
        else:
            website_dict = {self.DB_WEBSITE_NAME: item.get(self.JOB_WEBSITE_NAME),
                            self.DB_WEBSITE_URL: item.get(self.JOB_WEBSITE_URL),
                            self.DB_WEBSITE_LAST_UPDATED: today}
            return self.db.Websites.insert(website_dict)

    def handle_regions(self, item):
        """
        Create a new 'Region' object.
        If one with the same name already exists, update the 'last updated' to today.
        :return: The '_id' of the region.
        """
        today = datetime.now().strftime(time_format)
        region_ids = []
        for region_name in item.get(self.JOB_REGIONS):
            existing_region = self.db.Regions.find_one(
                {self.DB_REGION_NAME: region_name}
            )

            if existing_region:
                self.db.Regions.update(
                    {self.DB_ID: existing_region[self.DB_ID]},
                    {'$set': {self.DB_REGION_LAST_UPDATED: today}}
                )
                region_ids.append(existing_region[self.DB_ID])
            else:
                region_dict = {self.DB_REGION_NAME: region_name, self.DB_REGION_LAST_UPDATED: today}
                region_ids.append(self.db.Regions.insert(region_dict))
        return region_ids

    def handle_queries(self, item):
        """
        Create a new 'query' object.
        If one already exists, update the 'last updated' value to today.
        :return: The '_id' of the object.
        """
        today = datetime.now().strftime(time_format)
        query_ids = []
        for query_name in item.get(self.JOB_QUERIES):
            existing_query = self.db.Queries.find_one(
                {self.DB_QUERY_NAME: query_name}
            )
            if existing_query:
                self.db.Queries.update(
                    {self.DB_ID: existing_query[self.DB_ID]},
                    {'$set': {self.DB_QUERY_LAST_UPDATED: today}}
                )
                query_ids.append(existing_query[self.DB_ID])
            else:
                query_dict = {self.DB_QUERY_NAME: query_name, self.DB_QUERY_LAST_UPDATED: today}
                query_id = self.db.Queries.insert(query_dict)
                query_ids.append(query_id)
        return query_ids

    def handle_contact(self, item):
        """
        Create a new 'Contact' object.
        If one already exists, update the 'last updated' variable to today.
        :return: The '_id' of the object
        """
        today = datetime.now().strftime(time_format)
        contact_dict = {
            self.DB_CONTACT_NAME: item.get(self.JOB_CONTACT_NAME),
            self.DB_CONTACT_PHONE: item.get(self.JOB_CONTACT_PHONE),
            self.DB_CONTACT_FIELD: item.get(self.JOB_CONTACT_FIELD),
            self.DB_CONTACT_MAIL: item.get(self.JOB_CONTACT_MAIL)
        }

        existing_contact = self.db.Contacts.find_one(
            contact_dict
        )
        if existing_contact:
            self.db.Contacts.update(
                {self.DB_ID: existing_contact[self.DB_ID]},
                {'$set': {self.DB_QUERY_LAST_UPDATED: today}}
            )
            return existing_contact[self.DB_ID]
        else:
            contact_dict[self.DB_QUERY_LAST_UPDATED] = today
            return self.db.Contacts.insert(contact_dict)

    def handle_info(self, item):
        """
        Create a new info object.
        Contains other information, about the employer (!Important! this is not the contact of the 'seller')
        Also the email and the phone number which are not from the Sell Contact.
        If one already exists, update the 'last updated' value to today.
        :return: The '_id' of the object.
        """
        today = datetime.now().strftime(time_format)
        info_dict = {
            self.DB_INFO: item.get(self.JOB_INFO),
            self.DB_INFO_PHONE: item.get(self.JOB_INFO_PHONE),
            self.DB_INFO_MAIL: item.get(self.JOB_INFO_MAIL)
        }

        existing_info = self.db.Info.find_one(
            info_dict
        )
        if existing_info:
            self.db.Info.update(
                {self.DB_ID: existing_info[self.DB_ID]},
                {'$set': {self.DB_INFO_LAST_UPDATED: today}}
            )
            return existing_info[self.DB_ID]
        else:
            info_dict[self.DB_INFO_LAST_UPDATED] = today
            return self.db.Info.insert(info_dict)
