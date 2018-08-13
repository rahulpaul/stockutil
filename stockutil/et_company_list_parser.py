import os
import sys
import json
import sqlite3
import requests
import datetime
from enum import Enum
from http import HTTPStatus
from typing import List, Dict, Optional, NamedTuple
from bs4 import BeautifulSoup


ET_SERVER_URL = 'https://economictimes.indiatimes.com'
COMPANIES_LISTING_PATH = 'markets/stocks/stock-quotes'
BASE_URL = f'{ET_SERVER_URL}/{COMPANIES_LISTING_PATH}'

QUARTERLY_RESULT_URL = 'https://etfeedscache.indiatimes.com/ETServiceChartCompanyPage/GetCompanyQuaterData?charttype=&companyid={}&columnname={}&sortorder=ASC&callback=&nuofquarter=20&currencyformat=2&resulttype=nonconsolidate'
LIVE_FEED_URL = 'https://json.bselivefeeds.indiatimes.com/ET_Community/companypagedata?companyid={}'


def get_a_to_z_chr_list():
    a_to_z_ord = [ord('a') + i for i in range(26)]
    return [chr(x) for x in a_to_z_ord]


ticker_values = get_a_to_z_chr_list() + [str(i) for i in range(10)]

# proxies = {
#     'http': 'www-proxy.us.oracle.com',
#     'https': 'www-proxy.us.oracle.com'
# }

proxies = {}


def fetch_from_url(url: str) -> requests.Response:
    print('GET', url)
    return requests.get(url, proxies=proxies)


class FinancialResultFeature(Enum):

    Sales = ('salesTurnOver',)
    OtherIncome = ('otherIncomeValue',)
    OperatingProfit = ('operatingProfitValue',)
    OtherOperatingIncome = ('otherOperatingIncomeValue',)
    Ebita = ('ebitDAValue',)
    Interest = ('interestValue',)
    Depreciation = ('depreciationValue',)
    Tax = ('taxValue',)
    NetProfit = ('netProfitValue',)
    Eps = ('afterDilutedEPSValue',)

    def __init__(self, column_name):
        self.column_name = column_name


class CompanyNameAndUrl(NamedTuple):
    name: str
    et_url: str
    et_id: str

    @property
    def et_splits_page_url(self):
        return self.get_base_url_format().format('infocompanysplits')

    @property
    def et_dividends_page_url(self):
        return self.get_base_url_format().format('infocompanydividends')

    @property
    def et_bonus_page_url(self):
        return self.get_base_url_format().format('infocompanybonus')

    def get_base_url_format(self):
        t = self.et_url.split('/')
        t[-2] = '{}'
        return '/'.join(t)


class ShareHoldingPattern(NamedTuple):
    category: str
    no_of_shares: int
    percentage: float

    def to_json(self):
        return {'category': self.category, 'no_of_shares': self.no_of_shares, 'percentage': self.percentage}

    @classmethod
    def from_json(cls, data):
        return cls(
            category=data['category'],
            no_of_shares=data['no_of_shares'],
            percentage=data['percentage']
        )


class MFHolding(NamedTuple):
    fund_name: str
    category: str
    no_of_shares: int
    percent_change_in_shares: float
    percent_of_aum: float
    amount_invested_in_cr: float

    def to_json(self):
        return {'fund_name': self.fund_name, 'category': self.category, 'no_of_shares': self.no_of_shares,
                'percent_change_in_shares': self.percent_change_in_shares, 'percent_of_aum': self.percent_of_aum,
                'amount_invested_in_cr': self.amount_invested_in_cr}

    @classmethod
    def from_json(cls, data):
        return cls(
            fund_name=data['fund_name'],
            category=data['category'],
            no_of_shares=data['no_of_shares'],
            percent_change_in_shares=data['percent_change_in_shares'],
            percent_of_aum=data['percent_of_aum'],
            amount_invested_in_cr=data['amount_invested_in_cr']
        )


class QuarterlyResult(NamedTuple):
    result_value: float
    result_date: datetime.date

    DATE_FORMAT = '%Y-%m-%d'

    @classmethod
    def create(cls, r_value: float, r_date: str):
        result_date = datetime.datetime.strptime(r_date, cls.DATE_FORMAT).date()
        return cls(float(r_value), result_date)

    @property
    def r_date(self) -> str:
        return self.result_date.strftime(self.DATE_FORMAT)

    @property
    def r_year(self) -> int:
        return self.result_date.year

    @property
    def r_month(self) -> int:
        return self.result_date.month

    @property
    def r_day(self) -> int:
        return self.result_date.day

    def to_json(self):
        return {
            'value': self.result_value,
            'result_date': self.r_date,
            'result_year': self.r_year,
            'result_month': self.r_month
        }

    @classmethod
    def from_json(cls, data):
        return cls.create(data['value'], data['result_date'])


class BetaDuration(Enum):

    THREE_YEAR = ('3y', 'threeYearBeta')
    ONE_YEAR = ('1y', 'oneYearBeta')
    SIX_MONTH = ('6m', 'sixMonthBeta')
    THREE_MONTH = ('3m', 'threeMonthBeta')
    ONE_MONTH = ('1m', 'oneMonthBeta')

    def __init__(self, p_name, r_name):
        self.p_name = p_name
        self.r_name = r_name


class PageData:

    def __init__(self, company_name: str, et_id: str, mkt_cap_in_cr: Optional[float], pe_ratio: Optional[float],
                 pb_ratio: Optional[float], div_yield: Optional[float], face_value: Optional[float],
                 earning_per_share: Optional[float], book_value: Optional[float], low_52_week: Optional[float],
                 high_52_week: Optional[float], sector: str, industry: str, symbol: str, instrument: str,
                 beta: Dict[str, float], quarterly_results: Dict[FinancialResultFeature, List[QuarterlyResult]],
                 share_holding_pattern: List[ShareHoldingPattern], mutual_fund_holding: List[MFHolding]):

        self.company_name: str = company_name
        self.et_id: str = et_id
        self.mkt_cap_in_cr = mkt_cap_in_cr
        self.pe_ratio = pe_ratio
        self.pb_ratio = pb_ratio
        self.div_yield = div_yield
        self.face_value = face_value
        self.earning_per_share = earning_per_share
        self.book_value = book_value
        self.low_52_week = low_52_week
        self.high_52_week = high_52_week
        self.sector = sector
        self.industry = industry
        self.symbol = symbol
        self.instrument = instrument
        self.beta = beta
        self.quarterly_results = quarterly_results
        self.share_holding_pattern = share_holding_pattern
        self.mutual_fund_holding = mutual_fund_holding

    def to_json(self):
        data = {
            'company_name': self.company_name,
            'et_id': self.et_id,
            'symbol': self.symbol,
            'sector': self.sector,
            'industry': self.industry,
            'instrument': self.instrument,
            'mkt_cap_in_cr': self.mkt_cap_in_cr,
            'pe_ratio': self.pe_ratio,
            'pb_ratio': self.pb_ratio,
            'div_yield': self.div_yield,
            'face_value': self.face_value,
            'earning_per_share': self.earning_per_share,
            'book_value': self.book_value,
            'low_52_week': self.low_52_week,
            'high_52_week': self.high_52_week,
            'beta': self.beta,
            'quarterly_results': {k.name: list(map(lambda x: x.to_json(), v)) for k,v in self.quarterly_results.items()},
            'share_holding_pattern': list(map(lambda x: x.to_json(), self.share_holding_pattern)),
            'mutual_fund_holding': list(map(lambda x: x.to_json(), self.mutual_fund_holding))
        }

        return data

    @classmethod
    def from_json(cls, data):
        return cls(
            company_name=data['company_name'],
            et_id=data['et_id'],
            mkt_cap_in_cr=data['mkt_cap_in_cr'],
            pe_ratio=data['pe_ratio'],
            pb_ratio=data['pb_ratio'],
            div_yield=data['div_yield'],
            face_value=data['face_value'],
            earning_per_share=data['earning_per_share'],
            book_value=data['book_value'],
            low_52_week=data['low_52_week'],
            high_52_week=data['high_52_week'],
            sector=data['sector'],
            industry=data['industry'],
            symbol=data['symbol'],
            instrument=data['instrument'],
            beta=data['beta'],
            quarterly_results={FinancialResultFeature[k]: list(map(lambda x: QuarterlyResult.from_json(x), v)) for k,v in data['quarterly_results'].items()},
            share_holding_pattern=list(map(lambda x: ShareHoldingPattern.from_json(x), data['share_holding_pattern'])),
            mutual_fund_holding=list(map(lambda x: MFHolding.from_json(x), data['mutual_fund_holding']))
        )


def extract_company_id_from_url(company_url):
    company_id_prefix = 'companyid-'
    index1 = company_url.index(company_id_prefix)
    if index1 < 0:
        raise ValueError(f'CompanyURL {company_url} does not contain {company_id_prefix}')
    sub_str = company_url[index1 + len(company_id_prefix):]
    index2 = sub_str.index('.cms')
    if index2 < 0:
        raise ValueError(f'CompanyURL {company_url} does not contain .cms')
    return sub_str[:index2]


def extract_percent(text: str) -> float:
    index = text.index('%')
    if index > 0:
        text = text[: index]
    return float(text)


def get_listing(ticker: str) -> List[CompanyNameAndUrl]:
    page_url = f'{BASE_URL}?ticker={ticker}'
    resp = fetch_from_url(page_url)
    if HTTPStatus.OK != resp.status_code:
        raise Exception(f'response.statusCode={resp.status_code}')
    soup = BeautifulSoup(resp.text, 'html.parser')
    company_list_ul = soup.find('ul', class_='companyList')
    if not company_list_ul:
        return []
    items = company_list_ul.find_all('li')
    companies_list = []
    for item in items:
        name = item.a.text
        et_url = f"{ET_SERVER_URL}{item.a['href']}"
        try:
            company_id = extract_company_id_from_url(et_url)
        except ValueError as e:
            print(e.args[0], file=sys.stderr)
            company_id = ''
        companies_list.append(CompanyNameAndUrl(name=name, et_url=et_url, et_id=company_id))
    return companies_list


def write_company_lookup_info_to_csv(directory: str, ticker: str, listing: List[CompanyNameAndUrl]) -> None:
    os.makedirs(directory, exist_ok=True)
    file_name = f'{ticker}.csv'
    file_path = os.path.join(directory, file_name)
    if os.path.isfile(file_path):
        # delete the file
        print(f'File {file_path} already exists, deleting it ...')
        os.remove(file_path)
    with open(file_path, mode='w') as file:
        for item in listing:
            print(f'{item.name},{item.et_id},{item.et_url}', file=file)


def get_float_value(data_dict: Dict[str, str], key: str) -> Optional[float]:
    str_value = data_dict.get(key)
    if str_value is not None:
        return float(str_value)


def parse_page(company: CompanyNameAndUrl):
    quarterly_results_data = {}
    share_holding_data = []
    mf_listing_data = []
    page_data = PageData(company.name, company.et_id, None, None, None, None, None, None, None, None, None, None, None,
                         None, None, None, quarterly_results_data, share_holding_data, mf_listing_data)
    resp = fetch_from_url(company.et_url)
    if resp.status_code != HTTPStatus.OK:
        print(f'Could not access page for company {company.name}')
        return
    soup = BeautifulSoup(resp.text, 'html.parser')
    try:
        class_label_list = {'mkt_cap tar', 'p_e tar', 'p_b tar', 'div_yield tar'}
        data = {}
        i = soup.find('div', class_='d d1')
        children = i.ul.find_all('li')
        for li in children:
            try:
                spans = li.find_all('span')
                class_label = ' '.join(spans[1]['class'])
                if class_label in class_label_list:
                    data[class_label] = spans[1].text
            except:
                pass

        mkt_cap_in_cr = get_float_value(data, 'mkt_cap tar')
        pe_ratio = get_float_value(data, 'p_e tar')
        pb_ratio = get_float_value(data, 'p_b tar')
        div_yield = get_float_value(data, 'div_yield tar')
        # print(mkt_cap_in_cr, pe_ratio, pb_ratio, div_yield)
        page_data.mkt_cap_in_cr = mkt_cap_in_cr
        page_data.pe_ratio = pe_ratio
        page_data.pb_ratio = pb_ratio
        page_data.div_yield = div_yield
    except:
        pass

    try:
        class_label_list = {'face_value tar', 'eps_ttm tar', 'bv_sh tar', 'wk_lh tar nse_tab'}
        data = {}
        i = soup.find('div', class_='d d2')
        children = i.ul.find_all('li')
        for li in children:
            try:
                spans = li.find_all('span')
                class_label = ' '.join(spans[1]['class'])
                if class_label in class_label_list:
                    data[class_label] = spans[1].text
            except:
                pass

        face_value = get_float_value(data, 'face_value tar')
        earning_per_share = get_float_value(data, 'eps_ttm tar')
        book_value = get_float_value(data, 'bv_sh tar')
        low_high = data.get('wk_lh tar nse_tab')
        if low_high is not None:
            low, high = low_high.split('/')
            low_52_week = float(low)
            high_52_week = float(high)
        else:
            low_52_week, high_52_week = None, None
        # print(face_value, earning_per_share, book_value, low, high)
        page_data.face_value = face_value
        page_data.earning_per_share = earning_per_share
        page_data.book_value = book_value
        page_data.low_52_week = low_52_week
        page_data.high_52_week = high_52_week
    except:
        pass

    # get quarterly results data
    for feature in list(FinancialResultFeature):
        quarterly_result_url = QUARTERLY_RESULT_URL.format(company.et_id, feature.column_name)
        resp = fetch_from_url(quarterly_result_url)
        if resp.status_code != HTTPStatus.OK:
            print(f'Failed to get Quarterly result for {company.name}, feature = {feature.name}, '
                  f'url = {quarterly_result_url}', file=sys.stderr)
            continue
        data = resp.text
        data_dict = json.loads(data)
        raw_quarterly_data_list = data_dict['query']['results']['companyquarterdata']

        def _transform_raw_quarterly_data(raw_data):
            result_value_key_possibilities = {'columnvalue', 'colummvalue'}
            result_date_key = 'resultyear'
            if (all(result_value_key not in raw_data for result_value_key in result_value_key_possibilities)) \
                    or (result_date_key not in raw_data):
                raise ValueError(f'{raw_data} does not contain any of key {result_value_key_possibilities} or {result_date_key}')

            value = next(filter(lambda v: v is not None,
                                map(lambda k: raw_data.get(k), result_value_key_possibilities)))
            return QuarterlyResult.create(value, raw_data[result_date_key])

        feature_quarterly_results_data = list(map(_transform_raw_quarterly_data, raw_quarterly_data_list))
        # print(feature, '======>')
        # print(feature_quarterly_results_data)
        # quarterly_results_data[feature.name] = list(map(lambda x: x.to_json(), feature_quarterly_results_data))
        quarterly_results_data[feature] = feature_quarterly_results_data

    page_data.quarterly_results = quarterly_results_data

    # get share holding data
    try:
        share_holding_div = soup.find_all('div', {'id': 'chartTable'})[0]
        table_rows = share_holding_div.table.find_all('tr')
        for row in table_rows[1:]:
            try:
                col_data = row.find_all('td')
                category = col_data[0].text
                no_of_shares = int(col_data[1].text.replace(',', ''))
                percentage = float(col_data[2].text)
                data = ShareHoldingPattern(category, no_of_shares, percentage)
                share_holding_data.append(data)
                # print(data)
            except (IndexError, ValueError) as e:
                pass
    except IndexError:
        pass

    # page_dict['share_holding_pattern'] = list(map(lambda x: x.to_json(), share_holding_data))
    page_data.share_holding_pattern = share_holding_data

    # get mutual fund holding data
    mf_listing_divs = soup.find_all('div', class_='mfListData')
    for mf_div in mf_listing_divs:
        divs = mf_div.find_all('div', recursive=False)
        fund_name = divs[0].a['title']
        category = divs[1].text
        no_of_shares = int(divs[2].text.replace(',', ''))
        change_percent = extract_percent(divs[3].span.text)
        percent_of_aum = extract_percent(divs[4].text)
        try:
            invested_amount = float(divs[5].text)
        except ValueError:
            invested_amount = None
        data = MFHolding(fund_name, category, no_of_shares, change_percent, percent_of_aum, invested_amount)
        mf_listing_data.append(data)
        # print(data)

    # page_dict['mutual_fund_holding'] = list(map(lambda x: x.to_json(), mf_listing_data))
    page_data.mutual_fund_holding = mf_listing_data

    # parse info from live feed
    resp = fetch_from_url(LIVE_FEED_URL.format(company.et_id))
    if resp.status_code == HTTPStatus.OK:
        data = resp.json()
        segment_data = data['bseNseJson'][-1] if data.get('bseNseJson') else None
        company_beta = data['companyBeta'][-1] if data.get('companyBeta') else None

        if segment_data:
            page_data.sector = segment_data.get('sector')
            page_data.industry = segment_data.get('industry')
            page_data.symbol = segment_data.get('symbol')
            page_data.instrument = segment_data.get('instrument')

        if company_beta:
            page_data.beta = {bd.p_name: company_beta.get(bd.r_name) for bd in list(BetaDuration)}

    return page_data


class DataPersistenceService:

    et_companies = 'ET_COMPANY'
    et_companies_data = 'ET_COMPANY_DATA'

    et_company_create_cmd = f""" CREATE TABLE IF NOT EXISTS {et_companies} (
        id TEXT PRIMARY KEY,
        name TEXT,
        et_company_page_url TEXT,
        et_splits_page_url TEXT,
        et_dividends_page_url TEXT,
        et_bonus_page_url TEXT
    );        
    """

    et_company_data_create_cmd = f""" CREATE TABLE IF NOT EXISTS {et_companies_data} (
        id TEXT PRIMARY KEY,
        symbol TEXT,
        name TEXT,
        sector TEXT,
        industry TEXT,
        market_cap REAL,
        data TEXT        
    );
    """

    insert_into_et_company_sql = f"INSERT INTO {et_companies} VALUES (?, ?, ?, ?, ?, ?)"
    insert_into_et_company_data_sql = f"INSERT INTO {et_companies_data} VALUES (?, ?, ?, ?, ?, ?, ?)"

    select_from_et_company_sql = f"SELECT id, name, et_company_page_url FROM {et_companies} ORDER BY name LIMIT {{}} OFFSET {{}}"
    select_from_et_company_data_sql = f"SELECT data from {et_companies_data} WHERE id = '{{}}'"

    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = sqlite3.connect("etStocks.db")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    def create_tables(self):
        cursor = self.conn.cursor()
        cursor.execute(self.et_company_create_cmd)
        cursor.execute(self.et_company_data_create_cmd)
        self.conn.commit()

    def save_et_company(self, entry: CompanyNameAndUrl):
        data = (entry.et_id, entry.name, entry.et_url, entry.et_splits_page_url,
                entry.et_dividends_page_url, entry.et_bonus_page_url)
        cursor = self.conn.cursor()
        cursor.execute(self.insert_into_et_company_sql, data)
        self.conn.commit()

    def save_et_company_entries(self, entry_list: List[CompanyNameAndUrl]):
        data = [(entry.et_id, entry.name, entry.et_url, entry.et_splits_page_url, entry.et_dividends_page_url,
                 entry.et_bonus_page_url) for entry in entry_list]

        try:
            self.conn.executemany(self.insert_into_et_company_sql, data)
            self.conn.commit()
        except sqlite3.DatabaseError as e:
            self.conn.rollback()
            raise e

    def save_et_company_data_entries(self, entry_list: List[PageData]):
        data_list = []
        for entry in entry_list:
            json_str = json.dumps(entry.to_json())
            data = entry.et_id, entry.symbol, entry.company_name, entry.sector, entry.industry, entry.mkt_cap_in_cr, json_str
            data_list.append(data)

        self.conn.executemany(self.insert_into_et_company_data_sql, data_list)
        self.conn.commit()

    def fetch_et_companies(self, limit, offset) -> List[CompanyNameAndUrl]:
        cursor = self.conn.cursor()
        cursor.execute(self.select_from_et_company_sql.format(limit, offset))
        return list(map(lambda x: CompanyNameAndUrl(name=x[1], et_url=x[2], et_id=x[0]), cursor.fetchall()))

    def fetch_et_company_data(self, company: CompanyNameAndUrl) -> PageData:
        cursor = self.conn.cursor()
        cursor.execute(self.select_from_et_company_data_sql.format(company.et_id))
        data = cursor.fetchone()
        if data:
            return PageData.from_json(json.loads(data[0]))


def populate_et_company(persistence_service: DataPersistenceService):
    for ticker in ticker_values:
        company_listing = get_listing(ticker)
        try:
            persistence_service.save_et_company_entries(company_listing)
        except sqlite3.DatabaseError:
            # let us try to insert one entry at a time
            for company in company_listing:
                try:
                    persistence_service.save_et_company(company)
                except sqlite3.DatabaseError as e:
                    print(e, company)
    print('\nCompleted')


def populate_et_company_data(persistence_service: DataPersistenceService):

    def _parse_page_with_retry(c: CompanyNameAndUrl, max_retries, retry=0) -> PageData:
        import time
        try:
            return parse_page(c)
        except requests.ConnectionError as e:
            if retry > max_retries:
                raise e
            time.sleep(5)
            return _parse_page_with_retry(c, max_retries, retry+1)

    batch_size = 10
    offset = 0
    while True:
        company_list = persistence_service.fetch_et_companies(limit=batch_size, offset=offset)
        offset += batch_size
        data_list = []
        for company in company_list:
            page_data = persistence_service.fetch_et_company_data(company)
            if not page_data:
                try:
                    data_list.append(_parse_page_with_retry(company, 20))
                except requests.exceptions.ConnectionError as e:
                    raise e

        if data_list:
            persistence_service.save_et_company_data_entries(data_list)

        print(f"Parsing completed for {[company.name for company in company_list]}")
        print(f"offset = {offset}")
        if len(company_list) < batch_size:
            print("\nCompleted")
            break


def main():
    with DataPersistenceService() as persistence_service:
        persistence_service.create_tables()
        # populate_et_company(persistence_service)
        populate_et_company_data(persistence_service)


if __name__ == '__main__':
    main()
