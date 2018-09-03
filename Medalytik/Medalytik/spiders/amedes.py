import scrapy
from ..items import JobItem

query_id_map = {
    "Abrechnung": "31",
    "31": "Abrechnung",
    "Allgemeinmedizin": "110",
    "110": "Allgemeinmedizin",
    "Andrologie": "67",
    "67": "Andrologie",
    "Anmeldung": "78",
    "78": "Anmeldung",
    "BackOffice": "77",
    "77": "BackOffice",
    "Bakteriologie PCR": "26",
    "26": "Bakteriologie PCR",
    "Basislabor": "112",
    "112": "Basislabor",
    "Befundauskunft/Erfassung ": "120",
    "120": "Befundauskunft/Erfassung ",
    "Befunddruck & Versand": "43",
    "43": "Befunddruck & Versand",
    "Befundversand": "65",
    "65": "Befundversand",
    "Blutentnahme": "76",
    "76": "Blutentnahme",
    "Call-Center": "79",
    "79": "Call-Center",
    "Dermatologie": "149",
    "149": "Dermatologie",
    "Diabetologie": "81",
    "81": "Diabetologie",
    "Empfang": "85",
    "85": "Empfang",
    "Endokrinologie": "89",
    "89": "Endokrinologie",
    "gynäkologische Praxis": "148",
    "148": "gynäkologische Praxis",
    "gynäkologischer OP": "156",
    "156": "gynäkologischer OP",
    "Hämatologie": "48",
    "48": "Hämatologie",
    "hausärztliche Versorgung ": "124",
    "124": "hausärztliche Versorgung ",
    "Histologielaborbereich": "12",
    "12": "Histologielaborbereich",
    "Humangenetik": "29",
    "29": "Humangenetik",
    "Hygiene": "137",
    "137": "Hygiene",
    "IT": "3",
    "3": "IT",
    "IVF-Labor": "61",
    "61": "IVF-Labor",
    "Kernlabor": "106",
    "106": "Kernlabor",
    "klinische Chemie": "40",
    "40": "klinische Chemie",
    "Klinische Medizin": "104",
    "104": "Klinische Medizin",
    "Kompletterfassung": "10",
    "10": "Kompletterfassung",
    "Krankenhauslabor": "13",
    "13": "Krankenhauslabor",
    "Labor": "63",
    "63": "Labor",
    "Laboratoriumsmedizin": "21",
    "21": "Laboratoriumsmedizin",
    "Laborbüro": "126",
    "126": "Laborbüro",
    "Leistungsabrechnung": "150",
    "150": "Leistungsabrechnung",
    "Logistik": "5",
    "5": "Logistik",
    "Molekularbiologie": "99",
    "99": "Molekularbiologie",
    "Molekulare Diagnostik": "154",
    "154": "Molekulare Diagnostik",
    "Nuklearmedizin": "131",
    "131": "Nuklearmedizin",
    "Onkologie": "95",
    "95": "Onkologie",
    "onkologische Tagesklinik ": "134",
    "134": "onkologische Tagesklinik ",
    "OP-Assistenz": "155",
    "155": "OP-Assistenz",
    "Pädiatrie": "92",
    "92": "Pädiatrie",
    "Pathologie": "24",
    "24": "Pathologie",
    "Personal": "17",
    "17": "Personal",
    "Pflegedienst": "38",
    "38": "Pflegedienst",
    "Post-OP": "115",
    "115": "Post-OP",
    "Prä-/Postanalytik": "97",
    "97": "Prä-/Postanalytik",
    "Prä/-Postanalytik im Bereich Zytologie": "32",
    "32": "Prä/-Postanalytik im Bereich Zytologie",
    "Präanalytik": "8",
    "8": "Präanalytik",
    "Probenannahme": "57",
    "57": "Probenannahme",
    "Probenverteilung": "142",
    "142": "Probenverteilung",
    "Rechtsabteilung": "33",
    "33": "Rechtsabteilung",
    "Rheumatologie": "84",
    "84": "Rheumatologie",
    "Routinelabor": "18",
    "18": "Routinelabor",
    "SAP": "116",
    "116": "SAP",
    "Serologie": "41",
    "41": "Serologie",
    "Telefonplatz": "152",
    "152": "Telefonplatz",
    "Therapiebereich": "153",
    "153": "Therapiebereich",
    "Verfahrenstechnik": "132",
    "132": "Verfahrenstechnik",
    "Zytogenetik": "49",
    "49": "Zytogenetik",
    "Zytologie": "1",
    "1": "Zytologie"

}

url = "https://bewerberportal.amedes-group.com/amedesalsarbeitgeber/bewerberportal/stellenangebote.htm"
search_query = "tx_dmmjobcontrol_pi1%5Bsearch_submit%5D=Suchen"
id_query = "tx_dmmjobcontrol_pi1%5Bsearch%5D%5Bdiscipline%5D%5B%5D="
page_query = "tx_dmmjobcontrol_pi1%5Bpage%5D="


def url_from_query(query, page=1):
    return url + "?" + search_query + "&" + id_query + str(query) + "&" + page_query + str(page)


class AmedesSpider(scrapy.Spider):
    name = 'amedes'

    def __init__(self, queries):
        super(AmedesSpider, self).__init__(self.name)

        self.url = url

        self.website_name = "Amedes"
        self.website_url = self.url

        queries = queries.lstrip('[').rstrip(']').split(',')
        self.queries = []
        for query in queries:
            self.queries.append(query_id_map[query])

    def start_requests(self):
        for query in self.queries:
            yield scrapy.FormRequest(url_from_query(query),
                                     callback=self.parse,
                                     dont_filter=True,
                                     meta={'query': query,
                                           'page': 1})

    def parse(self, response):
        from scrapy.shell import inspect_response
        inspect_response(response, self)
        job_container_xpath = "//*[@class='dmmjobcontrol_list_item']"
        job_container_element = response.xpath(job_container_xpath)
        for job_container in job_container_element:
            for element in self.parse_job_container(job_container, response.meta['query']):
                yield element

        for element in self.load_next_page(response):
            yield element

    def load_next_page(self, response):
        next_page_element = response.xpath('//*[@class="dmmjobcontrol_pagebrowser_next"]')
        if next_page_element:
            query = response.meta['query']
            page = response.meta['page'] + 1
            yield scrapy.FormRequest(url_from_query(query, page),
                                     callback=self.parse,
                                     dont_filter=True,
                                     meta={'query': query,
                                           'page': page})

    def parse_job_container(self, job_container, query):
        job = JobItem()
        job['in_development'] = True
        job['website_name'] = "Amedes"
        job['website_url'] = "http://amedes-group.com"
        job['queries'] = [query_id_map[query]]

        region = job_container.xpath('//*[@class="dmmjobcontrol_list_regio"]').extract_first()
        if region:
            job['regions'] = [region.strip()]

        summary = job_container.xpath("//div[@class='dmmjobcontrol_list_short']").xpath("string()").extract_first()
        if summary:
            job['summary'] = summary.strip()

        title = job_container.xpath("//div[@class='dmmjobcontrol_list_title']").xpath("string()").extract_first()
        if title:
            job['title'] = title.strip()

        link = job_container.xpath("//div[@class='dmmjobcontrol_list_title']").xpath("h2/a/@href").extract_first()

        yield scrapy.Request(url=link, callback=self.parse_job_website, dont_filter=True, meta={'job': job})

    def parse_job_website(self, response):
        job = response.meta['job']
        job['url'] = response.request.url
        job = self.parse_date_availability(response, job)
        job = self.parse_environment(response, job)
        job = self.parse_description(response, job)
        job = self.parse_requirements(response, job)
        job = self.parse_offer(response, job)
        job = self.parse_info(response, job)
        job = self.parse_area(response, job)
        job = self.parse_contact(response, job)

        yield job

    @staticmethod
    def parse_date_availability(response, job):
        date = response.xpath('//*[@id="jobdetail_crdate"]/div/text()').extract_first()
        if date:
            job['date_availability'] = date.strip()
        return job

    @staticmethod
    def parse_environment(response, job):
        environment = response.xpath('//*[@id="jobdetail_employer_description"]').xpath('string()').extract_first()
        if environment:
            job['environment'] = environment.strip()
        return job

    @staticmethod
    def parse_description(response, job):
        description_list = response.xpath('//*[@id="jobdetail_job_description"]/div/ul/li/text()').extract()
        if description_list:
            job['desc'] = '- ' + '\n-'.join(description_list)
        return job

    @staticmethod
    def parse_requirements(response, job):
        requirements = response.xpath('//*[@id="jobdetail_job_requirements"]/div/ul/li/text()').extract()
        if requirements:
            job['requirements'] = '- ' + '\n-'.join(requirements)
        return job

    @staticmethod
    def parse_offer(response, job):
        offer = response.xpath('//*[@id="jobdetail_benefits"]/div/ul/li/text()').extract()
        if offer:
            job['offer'] = '- ' + '\n-'.join(offer)
        return job

    @staticmethod
    def parse_info(response, job):
        info = response.xpath('string(//*[@id="jobdetail_apply_information"]/div)').extract_first()
        if info:
            job['info'] = info.strip()
        return job

    @staticmethod
    def parse_area(response, job):
        area = response.xpath('//*[@id="right"]/div/div')
        for i in area:
            if i.xpath('span/text()').extract_first() == 'Kategorie':
                job['area'] = i.xpath('string()')
        return job

    @staticmethod
    def parse_contact(response, job):
        contact_name = response.xpath('//*[@id="jobdetail_rightcolumn_contactname"]/text()').extract_first()
        if contact_name:
            job['contact_name'] = " ".join(contact_name.split())
        contact_mail = response.xpath('//*[@id="jobdetail_rightcolumn_contactaddress"]/text()').extract_first()
        if contact_mail:
            job['contact_field'] = " ".join(contact_mail.split())
        contact_phone = response.xpath('//*[@id="jobdetail_rightcolumn_contactphone"]/text()').extract_first()
        if contact_phone:
            job['contact_phone'] = " ".join(contact_phone.split())
        return job
