# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError


def error_back(failure_):
    error_code = 0

    if failure_.check(HttpError):
        error_code = failure_.value.response.status

    elif failure_.check(DNSLookupError):
        error_code = -1

    elif failure_.check(TimeoutError, TCPTimedOutError):
        error_code = 504

    yield {'status': error_code}
