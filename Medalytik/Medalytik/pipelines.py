# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import datetime
import pymongo
from . import constants
import bson

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
    DB_ACTIVE = 'active'
    DB_AREA = 'area'
    DB_BENEFITS = 'benefits'
    DB_CONTACT_ID = 'contact_id'
    DB_CONTACT_NAME = 'name'
    DB_CONTACT_PHONE = 'phone'
    DB_CONTACT_FIELD = 'field'
    DB_CONTACT_MAIL = 'email'
    DB_CONTACT_LAST_UPDATED = 'last_updated'
    DB_DATE_AVAILABILITY = 'date_available'
    DB_DESCRIPTION = 'desc'
    DB_ENVIRONMENT = 'environment'
    DB_GROUP = 'group'
    DB_ID = '_id'
    DB_INFO = 'info'
    DB_INFO_PHONE = 'info_phone'
    DB_INFO_MAIL = 'info_email'
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
        self.stored_items = {}

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
        """Close the connection to prevent memory leaks. Also removes any old items"""
        self.obsolete_items()
        self.obsolete_database_object(self.db.Websites)
        self.obsolete_database_object(self.db.Contacts)
        self.obsolete_database_object(self.db.Queries)
        self.obsolete_database_object(self.db.Regions)
        self.client.close()

    def process_item(self, item, _):
        """
        Adds all the objects as documents {json like objects} to the Mongo database.
        Since the MongoDB is not RDBMS we create it ourselves.

        The existing objects are:
            - Websites
            - Regions
            - Queries
            - Contacts
            - Jobs

        First we try to get the existing Mongo object for each of the previously listed objects.
        If they do not exist we create a new Mongo object for each.
        Each object handling function:
            - website_id()
            - regions_id()
            - queries_id()
            - contact_id()
        Returns the id for the passed object.
        This id is then stored along the Job.

        Queries work differently as, multiple search terms can have the same job.
        For this reason, if an already uploaded job comes through here, we just update the query parameter.
        """

        # Push only jobs to the database which aren't in development
        if item.get("in_development"):
            return item

        today = datetime.now().strftime(time_format)

        # Check if the passed item is already stored.
        # This way we only have to update the query parameter.
        for stored_item, _id in self.stored_items.items():
            # Since we can not check for every single attribute we just check for the title and the website name.
            # If they are the same we treat the job as equals.
            if item.get(self.JOB_WEBSITE_NAME) == stored_item.get(self.JOB_WEBSITE_NAME) \
                    and item.get(self.JOB_TITLE) == stored_item.get(self.JOB_TITLE) \
                    and item.get(self.JOB_DATE_AVAILABILITY) == stored_item.get(self.JOB_DATE_AVAILABILITY) \
                    and item.get(self.JOB_REGIONS) == stored_item.get(self.JOB_REGIONS):
                # Item is already stored, just update the query and the date when it was updated. (Today)
                new_query_ids = self.queries_id(item)
                old_query_ids = self.queries_id(stored_item)
                query_ids = list(set(new_query_ids) | set(old_query_ids))

                self.db.Jobs.update_one({'_id': bson.ObjectId(_id)}, {'$set': {self.DB_QUERY_IDS: query_ids}})
                return item
        website_id = self.website_id(item)
        region_ids = self.regions_id(item)
        query_ids = self.queries_id(item)
        contact_id = self.contact_id(item)

        # Leave the query setting, since it is not complete.
        # A job can have multiple queries, but only one query is passed per item.
        # So me have to make sure the queries are up to date each day.
        job_dict = {
            self.DB_AREA: item.get(self.JOB_AREA),
            self.DB_ACTIVE: True,
            self.DB_ABOUT: item.get(self.JOB_ABOUT),
            self.DB_BENEFITS: item.get(self.JOB_BENEFITS),
            self.DB_CONTACT_ID: contact_id,
            self.DB_DATE_AVAILABILITY: item.get(self.JOB_DATE_AVAILABILITY),
            self.DB_DESCRIPTION: item.get(self.JOB_DESCRIPTION),
            self.DB_ENVIRONMENT: item.get(self.JOB_ENVIRONMENT),
            self.DB_GROUP: item.get(self.JOB_GROUP),
            self.DB_INFO: item.get(self.JOB_INFO),
            self.DB_INFO_PHONE: item.get(self.JOB_INFO_PHONE),
            self.DB_INFO_MAIL: item.get(self.JOB_INFO_MAIL),
            self.DB_LAST_UPDATED: today,
            self.DB_OFFER: item.get(self.JOB_OFFER),
            self.DB_REGION_IDS: region_ids,
            self.DB_REQUIREMENTS: item.get(self.JOB_REQUIREMENTS),
            self.DB_SUMMARY: item.get(self.JOB_SUMMARY),
            self.DB_TITLE: item.get(self.JOB_TITLE),
            self.DB_QUERY_IDS: query_ids,
            self.DB_URL: item.get(self.JOB_URL),
            self.DB_WEBSITE_ID: website_id,
        }

        existing_job = self.db.Jobs.find_one(
            {self.DB_WEBSITE_ID: website_id,
             self.DB_TITLE: item[self.JOB_TITLE]}
        )

        if existing_job:
            # Change the last updated only. This way we allow for user changes.
            self.db.Jobs.update(
                {self.DB_ID: bson.ObjectId(existing_job[self.DB_ID])},
                {'$set': job_dict}
            )
            _id = existing_job[self.DB_ID]
        else:
            _id = self.db.Jobs.insert(job_dict)

        self.stored_items[item] = _id
        return item

    def website_id(self, item):
        """
        Creates the website document for the item passed.
        If the document already exists, the 'last updated' value will be updated to today.
        :return: The id of the website for the job.
        """

        today = datetime.now().strftime(time_format)

        if not item.get(self.JOB_WEBSITE_NAME):
            existing_none_specified_website = self.db.Websites.find_one(
                {self.DB_WEBSITE_NAME: "None Specified"}
            )
            if existing_none_specified_website:
                self.db.Websites.update(
                    {self.DB_ID: bson.ObjectId(existing_none_specified_website[self.DB_ID])},
                    {'$set': {self.DB_WEBSITE_LAST_UPDATED: today}}
                )
                return existing_none_specified_website[self.DB_ID]
            else:
                none_website_specified_dict = {self.DB_WEBSITE_NAME: "None Specified",
                                               self.DB_WEBSITE_LAST_UPDATED: today}
                return self.db.Websites.insert(none_website_specified_dict)

        existing_website = self.db.Websites.find_one(
            {self.DB_WEBSITE_NAME: item.get(self.JOB_WEBSITE_NAME)}
        )

        if existing_website:
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

    def regions_id(self, item):
        """
        Create a new 'Region' object.
        If one with the same name already exists, update the 'last updated' to today.
        :return: The '_id' of the region.
        """
        today = datetime.now().strftime(time_format)

        if not item.get(self.JOB_REGIONS):
            existing_none_specified_region = self.db.Regions.find_one(
                {self.DB_REGION_NAME: "None Specified"}
            )
            if existing_none_specified_region:
                self.db.Regions.update(
                    {self.DB_ID: bson.ObjectId(existing_none_specified_region[self.DB_ID])},
                    {'$set': {self.DB_REGION_LAST_UPDATED: today}}
                )
                return [existing_none_specified_region[self.DB_ID]]
            else:
                none_region_specified_dict = {self.DB_REGION_NAME: "None Specified", self.DB_REGION_LAST_UPDATED: today}
                return [self.db.Regions.insert(none_region_specified_dict)]

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

    def queries_id(self, item):
        """
        Create a new 'query' object.
        If one already exists, update the 'last updated' value to today.
        :return: The '_id' of the object.
        """
        today = datetime.now().strftime(time_format)

        if not item.get(self.JOB_QUERIES):
            existing_none_specified_query = self.db.Queries.find_one(
                {self.DB_QUERY_NAME: "None Specified"}
            )
            if existing_none_specified_query:
                self.db.Queries.update(
                    {self.DB_ID: bson.ObjectId(existing_none_specified_query[self.DB_ID])},
                    {'$set': {self.DB_WEBSITE_LAST_UPDATED: today}}
                )
                return [existing_none_specified_query[self.DB_ID]]
            else:
                none_query_specified_dict = {self.DB_QUERY_NAME: "None Specified", self.DB_QUERY_LAST_UPDATED: today}
                return [self.db.Queries.insert(none_query_specified_dict)]

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

    def contact_id(self, item):
        """
        Create a new 'Contact' object.
        If one already exists, update the 'last updated' variable to today.
        :return: The '_id' of the object
        """
        today = datetime.now().strftime(time_format)

        if not item.get(self.JOB_CONTACT_FIELD)\
                and not item.get(self.JOB_CONTACT_MAIL) \
                and not item.get(self.JOB_CONTACT_NAME) \
                and not item.get(self.JOB_CONTACT_PHONE):
            existing_none_specified_contact = self.db.Contacts.find_one(
                {self.DB_CONTACT_NAME: "None Specified"}
            )
            if existing_none_specified_contact:
                self.db.Contacts.update(
                    {self.DB_ID: bson.ObjectId(existing_none_specified_contact[self.DB_ID])},
                    {'$set': {self.DB_CONTACT_LAST_UPDATED: today}}
                )
                return existing_none_specified_contact[self.DB_ID]
            else:
                none_contact_specified_dict = {self.DB_CONTACT_NAME: "None Specified",
                                               self.DB_CONTACT_LAST_UPDATED: today}
                return self.db.Contacts.insert(none_contact_specified_dict)

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

    def obsolete_items(self):
        jobs = self.db.Jobs.find()
        for job in jobs:
            website_id = job[self.DB_WEBSITE_ID]
            website = self.db.Websites.find_one({'_id': website_id})
            if not website:
                active = job[self.DB_LAST_UPDATED] == datetime.now().strftime(time_format)
            else:
                active = website[self.DB_WEBSITE_LAST_UPDATED] == job[self.DB_LAST_UPDATED]
            self.db.Jobs.update(
                {self.DB_ID: bson.ObjectId(job[self.DB_ID])},
                {'$set': {self.DB_ACTIVE: active}}
            )

    def obsolete_database_object(self, db_obj):
        objects = db_obj.find()
        for cur_object in objects:
            last_updated = cur_object[self.DB_LAST_UPDATED]
            today = datetime.now().strftime(time_format)
            active = last_updated == today
            db_obj.update(
                {self.DB_ID: bson.ObjectId(cur_object[self.DB_ID])},
                {'$set': {self.DB_ACTIVE: active}}
            )
