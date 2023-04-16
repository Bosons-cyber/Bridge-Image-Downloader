import os
import json
import time
import http.client
import ssl
import urllib.request
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


def get_image_data(soup):
    image_containers = soup.find_all('a', {'class': 'iusc'})
    image_data = [json.loads(container['m']) for container in image_containers]
    return image_data


def main():
    base_url = "https://www.bing.com"
    search_url = "https://www.bing.com/images/search?q=Rahmen+br%C3%BCcken+Vorderansicht+Bilder&form=IACMSM&first={}&cw=1848&ch=969"

    image_count = 0
    images_to_download = 200
    page_number = 1

    downloaded_urls = set()

    while image_count < images_to_download:
        url = search_url.format(page_number)
        soup = get_soup(url)

        image_data = get_image_data(soup)
        high_res_image_urls = [data["murl"] for data in image_data]

        for high_res_image_url in high_res_image_urls:
            if image_count >= images_to_download:
                break

            if high_res_image_url in downloaded_urls:
                print(f"Image downloaded, skipped: {high_res_image_url}")
                continue

            save_path = f"images/image_{image_count}.jpg"
            download_image(high_res_image_url, save_path)
            downloaded_urls.add(high_res_image_url)
            image_count += 1
            print(f"Downloaded images: {image_count}")

        page_number += 1
        time.sleep(1)


if __name__ == '__main__':
    main()
