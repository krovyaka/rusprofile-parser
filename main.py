import concurrent.futures
from re import findall

import requests
from bs4 import BeautifulSoup
from pymysql import connect

REQUIRED_OKVED = ["08.92.2", "42.91.10"]
THREAD_LIMIT = 5
RUS_PROFILE_URL = "https://rusprofile.ru"
USE_PROXY = False

proxyDict = {
    "https": "https://34.91.135.38:80",
}


# Database connection
def open_connection():
    return connect(
        host='localhost',
        port=3306,
        user='root',
        password='root',
        database='rusprofile'
    )


class Company:
    name: str
    ogrn: str
    okpo: str
    status: str
    registration_date: str
    initial_capital: int


def request_page(url, **kwargs):
    return requests.get(
        url=url,
        proxies=USE_PROXY and proxyDict or None,
        **kwargs
    )


def okved_to_url_part(okved: str) -> str:
    parts = okved.split(".")
    parts[2] = parts[2].ljust(2, '0')
    return ''.join(parts)


def urls_by_okved(okved):
    result = []
    html_source = request_page(RUS_PROFILE_URL + "/codes/" + okved_to_url_part(okved))
    bs = BeautifulSoup(html_source.content, "html.parser")

    items = bs.find("div", attrs={"class": "main-wrap__content"}) \
        .find_all("div", attrs={"class": "company-item"})

    for item in items:
        item: BeautifulSoup
        company_href: str = item.find("div", attrs={"class": "company-item__title"}) \
            .find("a").get("href").strip()
        result.append(RUS_PROFILE_URL + company_href)
    return result


def strip_text_if_exists(element: BeautifulSoup):
    if element:
        return element.text.strip()


def parse_company(company_url):
    result = Company()
    source = request_page(company_url)
    main_block = BeautifulSoup(source.content, "html.parser") \
        .find("div", attrs={"id": "anketa"})
    result.name = strip_text_if_exists(main_block.find("div", attrs={"class": "company-name"}))
    result.okpo = strip_text_if_exists(main_block.find("span", attrs={"id": "clip_okpo"}))
    result.ogrn = strip_text_if_exists(main_block.find("span", attrs={"id": "clip_ogrn"}))

    registration_date = strip_text_if_exists(main_block.find("dd", attrs={"itemprop": "foundingDate"}))
    result.registration_date = "-".join(registration_date.split(".").__reversed__())

    initial_capital = main_block.find("dt", text="Уставный капитал")
    if initial_capital:
        initial_capital = initial_capital.parent.find("span").text
        result.initial_capital = int(''.join(findall(r'\d', initial_capital)))
    else:
        result.initial_capital = None

    status = strip_text_if_exists(main_block.find("div", attrs={"class": "company-status"}))
    if status == "Действующая организация":
        result.status = "active"
    elif status == "Организация в процессе ликвидации":
        result.status = "closing"
    elif status == "Организация ликвидирована":
        result.status = "closed"

    return result


def company_exists_id_db(name: str):
    with open_connection() as connection:
        return bool(connection.execute("SELECT id FROM company WHERE name = '{0}'".format(name)))


def save_company(company: Company):
    with open_connection() as connection:
        sql = "INSERT INTO company (name, ogrn, okpo, status, registration_date, initial_capital) " \
              "VALUE ('{0}', {1}, {2}, '{3}', '{4}', {5})" \
            .format(company.name,
                    company.ogrn or "NULL",
                    company.okpo or "NULL",
                    company.status,
                    company.registration_date,
                    company.initial_capital or "NULL"
                    )
        print(sql)
        connection.execute(sql)


def main():
    urls_to_parse = []
    for okved in REQUIRED_OKVED:
        urls_to_parse += urls_by_okved(okved)

    print(urls_to_parse.__len__())
    with concurrent.futures.ThreadPoolExecutor(THREAD_LIMIT) as executor:
        for url, result in zip(urls_to_parse, executor.map(parse_company, urls_to_parse)):
            if not company_exists_id_db(result.name):
                save_company(result)


if __name__ == "__main__":
    main()
