from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import cloudscraper
import requests
import boto3
import csv

# URL to crawl
url = "https://phimmoitv.org/danh-sach/phim-le.html?page="
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Capcha': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
}

# Save to CSV in local
file = open("phimmoi.csv", "w", newline="", encoding="utf-8")
writer = csv.writer(file)
writer.writerow(["Title", "Status", "Director", "Country", "Year of Production", "Duration", "Total Episodes", "Quality", "Language", "Genre", "Views", "Description"])

# Parse HTML
for page_num in range(1, 3):
    response = requests.get(url + str(page_num), headers=headers)
    if(response.status_code == 200):
        print("Fetched page " + str(page_num))
        soup = BeautifulSoup(response.text, "html.parser")
        movies = soup.find_all("a", class_="movie-item m-block")
        for movie in movies:
            movie_title = movie.get("title")
            movie_url = movie.get("href")

            # Go to movie page
            response = requests.get(movie_url, headers=headers)
            if(response.status_code == 200):
                movie_soup = BeautifulSoup(response.text, "html.parser")

                # Get movie description
                movie_des = movie_soup.find_all("div", class_="content")
                movie_des = movie_des[0].find_all("p")
                # print(movie_des[0].text)

                # Get movie details
                movie_details = movie_soup.find_all("div", class_="movie-meta-info")
                details_dict = {}
                dd_array = []
                dd_elements = movie_details[0].find_all("dd")
                dt_elements = movie_details[0].find_all("dt")
                for i in range(len(dd_elements)):
                    details_dict[dt_elements[i].text.strip()] = dd_elements[i].text.strip()
                    dd_array.append(dd_elements[i].text.strip())

                writer.writerow([movie_title, dd_array[0], dd_array[1], dd_array[2], dd_array[3], dd_array[4], dd_array[5], dd_array[6], dd_array[7], dd_array[8], dd_array[9], movie_des[0].text])

            else:
                print("Failed to fetch movie " + movie_title)
                break
    else:
        print("Failed to fetch page " + str(page_num))
        break

# Upload file to S3
s3 = boto3.client('s3')
bucket_name = 'ai4e-ap-southeast-1-dev-s3-data-landing'
s3.upload_file('phimmoi.csv', bucket_name, 'golden_zone/nnlong/phimmoi.csv')


