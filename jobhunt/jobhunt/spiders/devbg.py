import scrapy
import sqlite3

connection = sqlite3.connect('mydatabase.db')

def check_tables(conn):
    cursor = conn.cursor()
    # SQL query to find all tables in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("Tables in the database:", tables)
    cursor.close()

def store(extracted, conn):
    print(extracted)
    cursor = conn.cursor()
    try:
        title = extracted['title']
        date_posted = extracted['date_posted']
        cursor.execute("INSERT INTO job_data VALUES(?, ?)", (title, date_posted))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("An error occurred:", e)
    finally:
        cursor.close()


class DevbgSpider(scrapy.Spider):
    name = "devbg"
    allowed_domains = ["dev.bg"]
    start_urls = ["https://dev.bg/company/jobs/python"]

    def parse(self, response):
        for listing in response.css('.listing-content-wrap'):
            title = listing.css('h6.job-title ::text').get()
            link = listing.css('a.overlay-link::attr(href)').get()

            if link:
                yield response.follow(link, self.parse_job_details,
                                      meta={'title': title, 'link': link})

    def parse_job_details(self, response):
        date_posted = response.css('.date-posted time::attr(datetime)').get()

        result = {
            'title': response.meta['title'],
            'date_posted': date_posted
        }

        store(result, connection)


check_tables(connection)