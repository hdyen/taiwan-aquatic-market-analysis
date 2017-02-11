import requests
import urllib

TOP = 500
SKIP = 0
# MARKET_NAME = '%E5%B2%A1%E5%B1%B1'
MARKET_NAME = u'岡山'
# TYPE_NAME = '%E8%99%B1%E7%9B%AE%E9%AD%9A'
TYPE_NAME = u'虱目魚'
START_DATE = '1041101'
END_DATE = '1041201'

API_BASE_URL = 'http://m.coa.gov.tw/OpenData/TaiwanAquaticTransDataCrawler.aspx?$top={}&$skip={}&MarketName={}&TypeName={}&StartDate={}&EndDate={}'

# request_url = API_BASE_URL.format(TOP, SKIP, MARKET_NAME, TYPE_NAME, START_DATE, END_DATE)

# http://m.coa.gov.tw/OpenData/AquaticTransData.aspx?$top=500&$skip=0&MarketName=%E5%B2%A1%E5%B1%B1&TypeName=%E8%99%B1%E7%9B%AE%E9%AD%9A&StartDate=1041101&EndDate=1041102