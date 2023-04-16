import os
import json
import time
import http.client
import ssl
import urllib.request
import socket
from bs4 import BeautifulSoup


def create_unverified_ssl_context():
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def get_soup(url):
    context = create_unverified_ssl_context()
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, context=context) as response:
        content = response.read()
    soup = BeautifulSoup(content, 'html.parser')
    return soup


def download_image(url, save_path):
    if os.path.exists(save_path):
        print(f"File already exists, skip download:{save_path}")
        return

    context = create_unverified_ssl_context()
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, context=context) as response, open(save_path, 'wb') as out_file:
            data = response.read()
            out_file.write(data)
    except urllib.error.URLError as e:
        print(f"Failed to download image: {url} -> {save_path}, reason: {e}")
    except http.client.RemoteDisconnected as e:
        print(f"Remote connection is closed: {url} -> {save_path}, reason: {e}")
    except ConnectionResetError as e:
        print(f"Connection reset by remote host: {url} -> {save_path}, reason: {e}")
    except socket.timeout as e:
        print(f"Download timeout: {url} -> {save_path}, reason: {e}")


def get_image_data(soup):
    image_containers = soup.find_all('a', {'class': 'iusc'})
    image_data = []
    for container in image_containers:
        m_data = container.get('m')
        if m_data:
            data = json.loads(m_data)
            image_data.append(data)
    return image_data


def main():
    base_url = "https://www.bing.com"

    image_count = 0
    images_per_page = 35
    socket.setdefaulttimeout(180)
    keyword_list = []
    next_turn = True
    while next_turn:
        search_url = "https://www.bing.com/images/search?q={}&form=AWIR&first={}&count={}"
        images_to_download = int(input("Please enter the number of images you want to search: "))
        page_number = int(input("\nPlease enter the page from which you want to download(from 0): "))

        user_query = input("\nPlease enter the search keywords: ")
        if user_query not in keyword_list:
            image_count = 0
            keyword_list.append(user_query)
        user_query = user_query.replace(" ", "+")
        url_encoded_query = urllib.parse.quote(user_query, safe='')
        search_url = search_url.format(url_encoded_query, "{}", images_per_page, "{}", "{}", "{}")
        print(search_url)

        query_directory = os.path.join("images", user_query)
        if not os.path.exists(query_directory):
            os.makedirs(query_directory)

        downloaded_urls = set()

        while image_count < images_to_download:
            start_index = images_per_page * page_number
            url = search_url.format(start_index, "{}")
            soup = get_soup(url)

            image_data = get_image_data(soup)
            high_res_image_urls = [urllib.parse.quote(data["murl"], safe=":/") for data in image_data]

            for high_res_image_url in high_res_image_urls:
                if image_count >= images_to_download:
                    break

                if high_res_image_url in downloaded_urls:
                    print(f"Image downloaded, skipped: {high_res_image_url}")
                    continue

                save_path = os.path.join(query_directory, f"image_{image_count}.jpg")
                download_image(high_res_image_url, save_path)
                downloaded_urls.add(high_res_image_url)
                image_count += 1
                print(f"Downloaded images: {image_count}")

            page_number += 1
            time.sleep(1)

        while True:
            answer = input("\nDo you want to download more?(y/n) ")
            if answer.lower() == 'y':
                next_turn = True
                break
            elif answer.lower() == 'n':
                next_turn = False
                break
            else:
                print("Invalid input, please re-enter.")


if __name__ == '__main__':
    main()
