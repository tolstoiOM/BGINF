from bs4 import BeautifulSoup
import requests
import re
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# Define default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'scrape_books',
    default_args=default_args,
    description='Scrape book data and store it in PostgreSQL',
    schedule_interval=timedelta(days=1),  # Run daily
)

def scrape_books():

    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="1234",
        host="host.docker.internal",
        port="5432"
    )

    head = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.goodreads.com/"
    }

    url = 'https://www.goodreads.com/list/show/205255.Goodreads_Editors_Book_Picks_for_2024?page=1&ref=ls_fl_0_seeall'
    page = requests.get(url, headers= head)
    soup = BeautifulSoup(page.text, 'html.parser')

    tableData = soup.find_all('td', attrs={"width": "100%"})
    scraped_books = []

    for data in tableData:
        authors = data.find("a", {"class": "authorName"}).text.strip()
        bookTitles = data.find("a", {"class": "bookTitle"}).text.strip()
        rating_text = data.find("span", {"class": "minirating"}).text.strip()
        
        avg_ratings = re.search(r"([\d.]+) avg rating", rating_text).group(1)
        num_ratings = re.search(r"— ([\d,]+) ratings", rating_text).group(1).replace(",", "")


        scraped_books.append({
            "title": bookTitles,
            "author": authors,
            "avg_rating": avg_ratings,
            "num_rating": num_ratings
        })

    cur = conn.cursor()

    insert_query = """
        INSERT INTO book_picks_2024 (title, author, avg_rating, num_rating)
        VALUES (%s, %s, %s, %s);
    """

    for book in scraped_books:
        cur.execute(insert_query, (book["title"], book["author"], book["avg_rating"], book["num_rating"]))
        print("Test")

    conn.commit()  # Save changes to the database
    cur.close()  # Close the cursor
    conn.close()  # Close the connection


scrape_task = PythonOperator(
    task_id='scrape_and_store_books',
    python_callable=scrape_books,
    dag=dag,
)


scrape_task