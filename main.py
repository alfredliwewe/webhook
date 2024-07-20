import requests
import time
from datetime import datetime
import mysql.connector

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="webhook"
)

cursor = connection.cursor(buffered=True)


# List of URLs to call
def get_urls():
    unix_timestamp = int(time.time())
    cursor.execute("SELECT * FROM links WHERE last_update + call_interval < %s", (unix_timestamp,))
    rows = cursor.fetchall()
    urls = []

    for row in rows:
        urls.append(row[1])
    return urls


def update_last_call(link):
    unix_timestamp = int(time.time())
    cursor.execute("UPDATE links SET last_update = %s WHERE link = %s", (unix_timestamp, link))
    connection.commit()


# Function to call URLs
def call_urls():
    urls = get_urls()
    for url in urls:
        try:
            response = requests.get(url)
            print(f"{datetime.now()} - URL: {url} - Status Code: {response.status_code}")
            print(response.text)
            isLast = False
            try:
                res = response.json()
                if(res['load_again']):
                    isLast= True
            except Exception as e:
                print(f"Error {e}")
            update_last_call(url)
        except requests.exceptions.RequestException as e:
            print(f"{datetime.now()} - URL: {url} - Error: {e}")


# Function to run the URL calls hourly
def run_hourly():
    while True:
        call_urls()
        get_links()
        time.sleep(5)  # Sleep for 1 hour (3600 seconds)
        pass
    pass


def get_links():
    try:
        res = requests.get("https://wikimalawi.com/webhook/").json()

        if res['status']:
            for row in res['links']:
                cursor.execute("SELECT * FROM links WHERE link = %s", (row['link'],))
                if len(cursor.fetchall()) == 0:
                    cursor.execute("INSERT INTO `links`(`id`, `link`, `call_interval`, `last_update`) VALUES (NULL, %s,"
                                   "%s, %s)",
                                   (row['link'], row['call_interval'], 0))
                    pass
                pass
            connection.commit()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    run_hourly()
