import scrapy
import sqlite3

DOMAIN = "https://dev.bg/company/jobs/python"
FILTER = "/?_job_location=sofiya%2Cremote&_seniority=mid-level"

connection = sqlite3.connect("job-hunt.db")


def create_table():
    conn = sqlite3.connect('job-hunt.db')
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS job_data (
        title TEXT,
        company TEXT,
        location TEXT,
        date_posted TEXT
    );
    """)
    conn.commit()
    conn.close()


def check_tables(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    if not tables:
        create_table()
    cursor.close()


def store(extracted, conn):
    print(extracted)
    cursor = conn.cursor()
    try:
        title = extracted["title"]
        company = extracted["company"]
        location = extracted["location"]
        date_posted = extracted["date_posted"]
        cursor.execute(
            "INSERT INTO job_data (title, company, location, date_posted) VALUES(?, ?, ?, ?)",
            (title, company, location, date_posted),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print("An error occurred:", e)
    finally:
        cursor.close()


def clean_text(text):
    return ' '.join(text.split())


class DevbgSpider(scrapy.Spider):
    name = "devbg"
    allowed_domains = ["dev.bg"]
    start_urls = [f"{DOMAIN + FILTER}"]

    def parse(self, response):
        for listing in response.css(".job-list-item"):
            title = listing.css("h6.job-title ::text").get()
            company = listing.css(".company-name ::text").get()
            link = listing.css("a.overlay-link::attr(href)").get()

            location = listing.css(".badge ::text").extract()
            location = ' '.join([clean_text(l) for l in location if
                                 l.strip()])  # Joins non-empty, cleaned text segments

            # Optional: refine extraction based on images or additional details
            if listing.css(".badge img[src*='remote-green.svg']"):
                location = "Fully Remote"
            elif listing.css(".badge img[src*='pin.png']"):
                location = location.strip()  # Assuming 'location' is something like 'София'

            if link:
                yield response.follow(
                    link,
                    self.parse_job_details,
                    meta={"title": title, "company": company, "location": location, "link": link},
                )

    def parse_job_details(self, response):
        date_posted = response.css(".date-posted time::attr(datetime)").get()

        result = {
            "title": response.meta["title"],
            "company": response.meta["company"],
            "location": response.meta["location"],
            "date_posted": date_posted,
        }

        store(result, connection)


check_tables(connection)
