import scrapy
import sqlite3
import atexit
import logging
import smtplib
import ssl

import os
from dotenv import load_dotenv

load_dotenv()


email_password = os.getenv("EMAIL_PASSWORD")

logging.basicConfig(level=logging.INFO)

DOMAIN = "https://dev.bg/company/jobs/python"
FILTER = "/?_job_location=sofiya%2Cremote&_seniority=mid-level"
_connection = None
new_jobs = []


def get_connection():
    global _connection
    if _connection is None:
        _connection = sqlite3.connect("job-hunt.db")
        atexit.register(close_connection)
    return _connection


def close_connection():
    global _connection
    if _connection:
        _connection.close()
        _connection = None
        logging.info("Database connection closed.")


def create_table():
    conn = get_connection()
    with conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_data (
                title TEXT,
                company TEXT,
                location TEXT,
                date_posted TEXT,
                link TEXT
            );
            """
        )
        conn.commit()


def check_existing_record(date_posted, title, company, location):
    conn = get_connection()
    with conn:
        cursor = conn.execute(
            """
                SELECT 1
                FROM job_data 
                WHERE date_posted = ? 
                AND title = ? 
                AND company = ? 
                AND location = ?
                """,
            (date_posted, title, company, location),
        )
        return cursor.fetchone() is not None


def send_email(subject, body):
    host = "smtp.gmail.com"
    port = 465
    username = "kalinpetrovbg@gmail.com"
    password = os.getenv("EMAIL_PASSWORD")

    if not password:
        logging.error("Email password not set in environment variables.")
        return

    receiver = username
    message = f"Subject: {subject}\n\n{body}"
    context = ssl.create_default_context()

    try:
        with smtplib.SMTP_SSL(host, port, context=context) as server:
            server.login(username, password)
            server.sendmail(username, receiver, message)
            logging.info("Email sent successfully.")
    except smtplib.SMTPException as e:
        logging.error(f"Failed to send email: {e}")


def store(extracted):
    conn = get_connection()
    try:
        title = extracted["title"]
        company = extracted["company"]
        location = extracted["location"]
        date_posted = extracted["date_posted"]
        link = extracted["link"]

        if not check_existing_record(date_posted, title, company, location):
            with conn:  # automatically commit or rollback
                query = """
                    INSERT INTO job_data 
                    (title, company, location, date_posted, link) 
                    VALUES (?, ?, ?, ?, ?)
                """
                params = (title, company, location, date_posted, link)
                conn.execute(query, params)
                logging.info(f"Record added: {title}")
                new_jobs.append(f"{title} - {company} - {location} - {date_posted}\n - {link}\n")
        else:
            logging.info(f"Duplicate found: {date_posted}, {company}")

    except sqlite3.DatabaseError as e:
        logging.error(f"Database error: {e}")


def finalize_and_send_emails():
    if new_jobs:
        subject = "New Job Listings"
        body = "\n".join(new_jobs)
        send_email(subject, body)
        logging.info("Email notification sent with new jobs.")


def check_location(location):
    location = " ".join([loc.strip() for loc in location])
    if "Hybrid" in location:
        location = "Hybrid"
    elif "Remote" in location:
        location = "Remote"
    else:
        location = "On site"
    return location


class DevBgSpider(scrapy.Spider):
    name = "devbg"
    allowed_domains = ["dev.bg"]
    start_urls = [f"{DOMAIN + FILTER}"]

    def closed(self, reason):
        finalize_and_send_emails()  # Send email after spider is closed

    def parse(self, response):
        for listing in response.css(".job-list-item"):
            title = listing.css("h6.job-title ::text").get()
            company = listing.css(".company-name ::text").get()
            link = listing.css("a.overlay-link::attr(href)").get()
            raw_location = listing.css(".badge ::text").extract()
            location = check_location(raw_location)

            if link:
                yield response.follow(
                    link,
                    self.parse_job_details,
                    meta={
                        "title": title,
                        "company": company,
                        "location": location,
                        "link": link,
                    },
                )

    def parse_job_details(self, response):
        date_posted = response.css(".date-posted time::attr(datetime)").get()

        result = {
            "title": response.meta["title"],
            "company": response.meta["company"],
            "location": response.meta["location"],
            "date_posted": date_posted,
            "link": response.meta["link"]
        }

        store(result)


create_table()
