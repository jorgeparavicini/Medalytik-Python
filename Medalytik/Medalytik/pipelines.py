# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import datetime
import pymongo
from . import mongoConstants
import bson

time_format = '%d %m %Y'


class MongoDBPipeline(object):
    """
    Connects to a Mongo Database and writes all the jobs in json like format to it.
    To Change the connection to the database change the 'MONGO URI' and 'MONGO DB' values from the settings.
    Also change the collection value below.
    """

    COLLECTION_NAME = 'Medalytik'
    MONGO_RELEASE_DATABASE_SETTINGS_NAME = 'MONGO_RELEASE_DB'
    MONGO_DEBUG_DATABASE_SETTINGS_NAME = 'MONGO_DEBUG_DB'

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

    @property
    def today(self):
        return datetime.now().strftime(time_format)

    def __init__(self, mongo_debug_uri, mongo_release_uri, mongo_debug_db, mongo_release_db):
        self.mongo_debug_uri = mongo_debug_uri
        self.mongo_debug_db = mongo_debug_db
        self.debug_client = None
        self.debug_db = None

        self.mongo_release_uri = mongo_release_uri
        self.mongo_release_db = mongo_release_db
        self.release_client = None
        self.release_db = None

        self.stored_debug_items = {}
        self.stored_release_items = {}

    @classmethod
    def from_crawler(cls, crawler):
        """Initiate the URI and database strings."""
        return cls(
            mongo_debug_uri=mongoConstants.MONGO_DEBUG_URI,
            mongo_release_uri=mongoConstants.MONGO_RELEASE_URI,
            mongo_debug_db=crawler.settings.get(MongoDBPipeline.MONGO_DEBUG_DATABASE_SETTINGS_NAME),
            mongo_release_db=crawler.settings.get(MongoDBPipeline.MONGO_RELEASE_DATABASE_SETTINGS_NAME)
        )

    def open_spider(self, _):
        """Connect to the Mongo Database."""
        self.debug_client = pymongo.MongoClient(self.mongo_debug_uri)
        self.debug_db = self.debug_client[self.mongo_debug_db]

        self.release_client = pymongo.MongoClient(self.mongo_release_uri)
        self.release_db = self.release_client[self.mongo_release_db]

    def close_spider(self, _):
        """Close the connection to prevent memory leaks. Also removes any old items"""
        self.obsolete_items(self.debug_db)
        self.obsolete_database_object(self.debug_db.Websites)
        self.obsolete_database_object(self.debug_db.Contacts)
        self.obsolete_database_object(self.debug_db.Queries)
        self.obsolete_database_object(self.debug_db.Regions)
        self.debug_client.close()

        self.obsolete_items(self.release_db)
        self.obsolete_database_object(self.release_db.Websites)
        self.obsolete_database_object(self.release_db.Contacts)
        self.obsolete_database_object(self.release_db.Queries)
        self.obsolete_database_object(self.release_db.Regions)
        self.release_client.close()

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

        # Already stored job
        stored_item = self.update_stored_item(item)
        if stored_item:
            return stored_item

        # Job has not yet been saved today

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
            self.DB_LAST_UPDATED: self.today,
            self.DB_OFFER: item.get(self.JOB_OFFER),
            self.DB_REGION_IDS: region_ids,
            self.DB_REQUIREMENTS: item.get(self.JOB_REQUIREMENTS),
            self.DB_SUMMARY: item.get(self.JOB_SUMMARY),
            self.DB_TITLE: item.get(self.JOB_TITLE),
            self.DB_QUERY_IDS: query_ids,
            self.DB_URL: item.get(self.JOB_URL),
            self.DB_WEBSITE_ID: website_id,
        }

        if item.get("in_development"):
            db = self.debug_db
            stored_items = self.stored_debug_items
        else:
            db = self.release_db
            stored_items = self.stored_release_items

        existing_job = db.Jobs.find_one(
            {self.DB_WEBSITE_ID: website_id,
             self.DB_TITLE: item[self.JOB_TITLE]}
        )

        if existing_job:
            # Change the last updated only. This way we allow for user changes.
            db.Jobs.update(
                {self.DB_ID: bson.ObjectId(existing_job[self.DB_ID])},
                {'$set': job_dict}
            )
            _id = existing_job[self.DB_ID]
        else:
            _id = db.Jobs.insert(job_dict)

        stored_items[item] = _id
        return item

    def update_stored_item(self, item):
        # Check if the passed item is already stored.
        # This way we only have to update the query parameter.

        if item.get("in_development"):
            stored_item_list = self.stored_debug_items
        else:
            stored_item_list = self.stored_release_items

        for stored_item, _id in stored_item_list.items():
            # Since we can not check for every single attribute we just check for the title and the website name.
            # If they are the same we treat the job as equals.
            if item.get(self.JOB_WEBSITE_NAME) == stored_item.get(self.JOB_WEBSITE_NAME) \
                    and item.get(self.JOB_TITLE) == stored_item.get(self.JOB_TITLE) \
                    and item.get(self.JOB_DATE_AVAILABILITY) == stored_item.get(self.JOB_DATE_AVAILABILITY) \
                    and item.get(self.JOB_REGIONS) == stored_item.get(self.JOB_REGIONS):
                # Item is already stored, just update the query.
                # Merge old queries with new ones.
                new_query_ids = self.queries_id(item)
                old_query_ids = self.queries_id(stored_item)
                query_ids = list(set(new_query_ids) | set(old_query_ids))

                # Parse correct database.
                if item.get("in_development"):
                    db = self.debug_db
                else:
                    db = self.release_db

                db.Jobs.update_one({'_id': bson.ObjectId(_id)}, {'$set': {self.DB_QUERY_IDS: query_ids}})
                return item

    def website_id(self, item):
        """
        Creates the website document for the item passed.
        If the document already exists, the 'last updated' value will be updated to today.
        :return: The id of the website for the job.
        """

        if item.get("in_development"):
            db = self.debug_db
        else:
            db = self.release_db

        # No website has been specified
        if not item.get(self.JOB_WEBSITE_NAME):
            # Parse existing non specified website
            existing_none_specified_website = db.Websites.find_one(
                {self.DB_WEBSITE_NAME: "None Specified"}
            )
            if existing_none_specified_website:
                # Update last updated
                db.Websites.update(
                    {self.DB_ID: bson.ObjectId(existing_none_specified_website[self.DB_ID])},
                    {'$set': {self.DB_WEBSITE_LAST_UPDATED: self.today}}
                )
                return existing_none_specified_website[self.DB_ID]
            else:
                # Create new none specified website
                none_website_specified_dict = {self.DB_WEBSITE_NAME: "None Specified",
                                               self.DB_WEBSITE_LAST_UPDATED: self.today}
                return db.Websites.insert(none_website_specified_dict)

        # Website has been specified, parse it from the database
        existing_website = db.Websites.find_one(
            {self.DB_WEBSITE_NAME: item.get(self.JOB_WEBSITE_NAME)}
        )

        # Website already exists in database
        if existing_website:
            # Update last updated time.
            if existing_website[self.DB_WEBSITE_LAST_UPDATED] != self.today:
                db.Websites.update(
                    {self.DB_ID: existing_website[self.DB_ID]},
                    {'$set': {self.DB_WEBSITE_LAST_UPDATED: self.today}}
                )
            return existing_website[self.DB_ID]
        else:
            # Website does not yet exist in database
            website_dict = {self.DB_WEBSITE_NAME: item.get(self.JOB_WEBSITE_NAME),
                            self.DB_WEBSITE_URL: item.get(self.JOB_WEBSITE_URL),
                            self.DB_WEBSITE_LAST_UPDATED: self.today}
            return db.Websites.insert(website_dict)

    def regions_id(self, item):
        """
        Create a new 'Region' object.
        If one with the same name already exists, update the 'last updated' to today.
        :return: The '_id' of the region.
        """

        if item.get("in_development"):
            db = self.debug_db
        else:
            db = self.release_db

        # No region has been specified
        if not item.get(self.JOB_REGIONS):
            existing_none_specified_region = db.Regions.find_one(
                {self.DB_REGION_NAME: "None Specified"}
            )
            if existing_none_specified_region:
                db.Regions.update(
                    {self.DB_ID: bson.ObjectId(existing_none_specified_region[self.DB_ID])},
                    {'$set': {self.DB_REGION_LAST_UPDATED: self.today}}
                )
                return [existing_none_specified_region[self.DB_ID]]
            else:
                none_region_specified_dict = {self.DB_REGION_NAME: "None Specified",
                                              self.DB_REGION_LAST_UPDATED: self.today}
                return [db.Regions.insert(none_region_specified_dict)]

        region_ids = []
        for region_name in item.get(self.JOB_REGIONS):
            existing_region = db.Regions.find_one(
                {self.DB_REGION_NAME: region_name}
            )

            if existing_region:
                db.Regions.update(
                    {self.DB_ID: existing_region[self.DB_ID]},
                    {'$set': {self.DB_REGION_LAST_UPDATED: self.today}}
                )
                region_ids.append(existing_region[self.DB_ID])
            else:
                region_dict = {self.DB_REGION_NAME: region_name, self.DB_REGION_LAST_UPDATED: self.today}
                region_ids.append(db.Regions.insert(region_dict))
        return region_ids

    def queries_id(self, item):
        """
        Create a new 'query' object.
        If one already exists, update the 'last updated' value to today.
        :return: The '_id' of the object.
        """

        if item.get("in_development"):
            db = self.debug_db
        else:
            db = self.release_db

        if not item.get(self.JOB_QUERIES):
            existing_none_specified_query = db.Queries.find_one(
                {self.DB_QUERY_NAME: "None Specified"}
            )
            if existing_none_specified_query:
                db.Queries.update(
                    {self.DB_ID: bson.ObjectId(existing_none_specified_query[self.DB_ID])},
                    {'$set': {self.DB_WEBSITE_LAST_UPDATED: self.today}}
                )
                return [existing_none_specified_query[self.DB_ID]]
            else:
                none_query_specified_dict = {self.DB_QUERY_NAME: "None Specified",
                                             self.DB_QUERY_LAST_UPDATED: self.today}
                return [db.Queries.insert(none_query_specified_dict)]

        query_ids = []
        for query_name in item.get(self.JOB_QUERIES):
            existing_query = db.Queries.find_one(
                {self.DB_QUERY_NAME: query_name}
            )
            if existing_query:
                db.Queries.update(
                    {self.DB_ID: existing_query[self.DB_ID]},
                    {'$set': {self.DB_QUERY_LAST_UPDATED: self.today}}
                )
                query_ids.append(existing_query[self.DB_ID])
            else:
                query_dict = {self.DB_QUERY_NAME: query_name, self.DB_QUERY_LAST_UPDATED: self.today}
                query_id = db.Queries.insert(query_dict)
                query_ids.append(query_id)
        return query_ids

    def contact_id(self, item):
        """
        Create a new 'Contact' object.
        If one already exists, update the 'last updated' variable to today.
        :return: The '_id' of the object
        """

        if item.get("in_development"):
            db = self.debug_db
        else:
            db = self.release_db

        if not item.get(self.JOB_CONTACT_FIELD)\
                and not item.get(self.JOB_CONTACT_MAIL) \
                and not item.get(self.JOB_CONTACT_NAME) \
                and not item.get(self.JOB_CONTACT_PHONE):
            existing_none_specified_contact = db.Contacts.find_one(
                {self.DB_CONTACT_NAME: "None Specified"}
            )
            if existing_none_specified_contact:
                db.Contacts.update(
                    {self.DB_ID: bson.ObjectId(existing_none_specified_contact[self.DB_ID])},
                    {'$set': {self.DB_CONTACT_LAST_UPDATED: self.today}}
                )
                return existing_none_specified_contact[self.DB_ID]
            else:
                none_contact_specified_dict = {self.DB_CONTACT_NAME: "None Specified",
                                               self.DB_CONTACT_LAST_UPDATED: self.today}
                return db.Contacts.insert(none_contact_specified_dict)

        contact_dict = {
            self.DB_CONTACT_NAME: item.get(self.JOB_CONTACT_NAME),
            self.DB_CONTACT_PHONE: item.get(self.JOB_CONTACT_PHONE),
            self.DB_CONTACT_FIELD: item.get(self.JOB_CONTACT_FIELD),
            self.DB_CONTACT_MAIL: item.get(self.JOB_CONTACT_MAIL)
        }

        existing_contact = db.Contacts.find_one(
            contact_dict
        )
        if existing_contact:
            db.Contacts.update(
                {self.DB_ID: existing_contact[self.DB_ID]},
                {'$set': {self.DB_QUERY_LAST_UPDATED: self.today}}
            )
            return existing_contact[self.DB_ID]
        else:
            contact_dict[self.DB_QUERY_LAST_UPDATED] = self.today
            return db.Contacts.insert(contact_dict)

    def obsolete_items(self, db):
        jobs = db.Jobs.find()
        for job in jobs:
            website_id = job[self.DB_WEBSITE_ID]
            website = db.Websites.find_one({'_id': website_id})
            if not website:
                active = job[self.DB_LAST_UPDATED] == datetime.now().strftime(time_format)
            else:
                active = website[self.DB_WEBSITE_LAST_UPDATED] == job[self.DB_LAST_UPDATED]
            db.Jobs.update(
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
