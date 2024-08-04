import scrapy


class DevbgSpider(scrapy.Spider):
    name = "devbg"
    allowed_domains = ["dev.bg"]
    start_urls = ["https://dev.bg/company/jobs/python"]

    def parse(self, response):
        for listing in response.css('.listing-content-wrap'):
            yield {
                'title': listing.css('h6.job-title ::text').get(),
                'link': listing.css('a.overlay-link::attr(href)').get()
            }