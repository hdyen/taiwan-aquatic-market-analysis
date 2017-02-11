import scrapy


class AquaticTransFormSpider(scrapy.Spider):
    name = 'AquaticTransSpider'
    start_urls = ['http://m.coa.gov.tw/outside/AquaticTrans/Search.aspx']

    markets = dict()
    fish_types = dict()

    def parse(self, response):

        for selector in response.css('select#ctl00_Main_Market > option'):
            market_id = selector.xpath('@value').extract()[0]
            market_name = selector.xpath('text()').extract()[0].encode('utf-8')
            self.markets[market_id] = market_name.rstrip()

        for selector in response.css('select#ctl00_Main_FishType > option'):
            fish_id = selector.xpath('@value').extract()[0]
            fish_name = selector.xpath('text()').extract()[0].encode('utf-8')
            self.fish_types[fish_id] = fish_name.rstrip()

        yield {'markets': self.markets, 'fish_types': self.fish_types}



