import re
import json
from pathlib import Path

from urllib.parse import quote
from urllib.parse import unquote


def custom_quote(string: str) -> str:
    return quote(string, ' -_.!*()').replace('-', '+').replace(' ', '-')


def custom_unquote(url: str) -> str:
    return unquote(url.replace('-', ' ').replace('+', '-'))


class UrlParser(object):

    JOB_WORDS = json.load((Path(__file__).resolve().parent / "job_words.json").open("rt", encoding="utf-8"))

    EXTRACT_INFO_REGEX = {
        "keyword": "^/{sw}-?([^/?]+)/?[^?/]*$",
        "region": "^/{sw}[^/?]*/([^?/]+)$",
        "region_main": r"^/{sw}/([^?]+)\?main$",
        "comp": r"^/{sw}-([^?/]+)\?comp$",
        "cdp": r"^/company/-?\d+/([^?]+)$",
    }
    REGEX_FOR_TYPE = {
        "serp": r"^/{sw}[^/?]*/?[^?/]*$",
        "region_main": r"^/{sw}/[^?]+\?main$",
        "comp": r"^/{sw}[^?]+\?comp$",
        "cdp": r"^/company/-?\d+/[^?]+$",
        "jdp": r"^/jdp/-?\d+/[^?]+$",
        "desc": r"^/desc/-?\d+/[^?]+$",
        "away": r"^/away/-?\d+/[^?]+$",
        "with_param": r"^[^?]+\?.+$"
    }
    URL_TYPES = {
        1: 'serp',
        2: 'region_main',
        3: 'comp',
        4: 'cdp',
        5: 'jdp',
        6: 'desc',
        7: 'away',
        8: 'with_param',
        9: 'other'
    }

    def __init__(self, country: str):
        self.country = country

    def parse(self, url_orig: str) -> dict:
        url = self.remove_domain(url_orig.strip())
        url_type, type_id = self._get_url_type(url=url)

        response = dict()
        response['url'] = url
        response['type'] = url_type
        response['type_id'] = type_id
        response['keyword'] = self._extract_keyword(url=url, url_type=url_type)
        response['region'] = self._extract_region(url=url, url_type=url_type)
        response['company'] = self._extract_company(url=url, url_type=url_type)

        return response

    def _extract_keyword(self, url: str, url_type: str):
        if url_type not in ['serp']:
            return None

        regex_name = "keyword"
        for sw in self.__search_words:
            fkw = re.search(self.EXTRACT_INFO_REGEX[regex_name].format(sw=sw), url, re.I | re.S)
            if fkw and fkw.group(1):
                return custom_unquote(fkw.group(1).strip())

    def _extract_region(self, url: str, url_type: str):
        if url_type not in ['serp', 'region_main']:
            return None

        regex_name = "region" if url_type == "serp" else "region_main"
        for sw in self.__search_words:
            fr = re.search(self.EXTRACT_INFO_REGEX[regex_name].format(sw=sw), url, re.I | re.S)
            if fr and fr.group(1):
                return custom_unquote(fr.group(1))

    def _extract_company(self, url: str, url_type: str):
        if url_type not in ['comp', 'cdp']:
            return None

        regex_name = url_type
        for sw in self.__search_words:
            fco = re.search(self.EXTRACT_INFO_REGEX[regex_name].format(sw=sw), url, re.I | re.S)
            if fco and fco.group(1):
                return custom_unquote(fco.group(1))

    def _get_url_type(self, url: str):
        # find types with search word
        for sw in self.__search_words:
            if re.search(self.REGEX_FOR_TYPE["serp"].format(sw=sw), url, re.I | re.S):
                return "serp", 1
            elif re.search(self.REGEX_FOR_TYPE["region_main"].format(sw=sw), url, re.I | re.S):
                return "region_main", 2
            elif re.search(self.REGEX_FOR_TYPE["comp"].format(sw=sw), url, re.I | re.S):
                return "comp", 3

        if re.search(self.REGEX_FOR_TYPE["cdp"], url, re.I | re.S):
            return "cdp", 4
        elif re.search(self.REGEX_FOR_TYPE["jdp"], url, re.I | re.S):
            return "jdp", 5
        elif re.search(self.REGEX_FOR_TYPE["desc"], url, re.I | re.S):
            return "desc", 6
        elif re.search(self.REGEX_FOR_TYPE["away"], url, re.I | re.S):
            return "away", 7
        elif re.search(self.REGEX_FOR_TYPE["with_param"], url, re.I | re.S):
            return "with_param", 8
        else:
            return "other", 9

    @property
    def country(self):
        return self.__country

    @country.setter
    def country(self, new_country: str):
        self.__search_words = list()
        for sw in self.JOB_WORDS[new_country.lower().strip()]:
            self.__search_words.append(custom_quote(sw))
        self.__country = new_country

    @staticmethod
    def remove_domain(url: str) -> str:
        url = re.sub(r"https?://[^/]+", "", url, re.I | re.S)
        return re.sub("^/m/", "/", url, re.I | re.S)
