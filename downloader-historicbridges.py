import csv
import os
import concurrent.futures
import time
import urllib.request
import urllib.parse
from urllib.parse import urljoin
import urllib.error
from bs4 import BeautifulSoup
import ssl
import requests
import logging
import json
import asyncio
import aiofiles
from logging.handlers import RotatingFileHandler

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

with open('config_his.json', 'r') as config_file:
    config = json.load(config_file)

BASE_URL = config['BASE_URL']
USER_AGENT = config['USER_AGENT']
IMAGE_FOLDER = config['IMAGE_FOLDER']
summary_csv_path = config['summary_csv_path']
time_lag = config['time_lag']
total_workers = config['total_workers']

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler('bridge_downloader_his.log', maxBytes=10000, backupCount=5)
    ]
)


def get_full_bridge_url(country_code, base_url):
    if country_code == "ZHEJIANG":
        final_address = f"{base_url}/b_a_list.php?ct=China&c=&ptype=state&pname={country_code}"
    else:
        final_address = f"{base_url}/b_a_list.php?ct=&c=&ptype=country&pname={country_code}"
    return final_address


def get_bridge_images(soup):
    images = []
    seen_urls = set()

    for img in soup.find_all('img', class_='blackborders'):
        img_url = img.get('src')
        full_url = urljoin(BASE_URL, img_url)
        if img_url and full_url not in seen_urls:
            seen_urls.add(full_url)
            images.append(full_url)

    return images


def list_supported_countries():
    try:
        with open("country_codes_his.json", "r", encoding="utf-8") as file:
            country_codes = json.load(file)

        print("Supported country codes:")
        for code, name in country_codes.items():
            print(f"{code}: {name}")
    except FileNotFoundError:
        print("Country codes file not found.")
    except json.JSONDecodeError:
        print("Error decoding JSON from the country codes file.")
    except Exception as e:
        print(f"Error reading country codes file: {e}")


def create_folder(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


def create_bridge_folder(bridge_name):
    bridge_folder = os.path.join(IMAGE_FOLDER, bridge_name)
    create_folder(bridge_folder)

    return bridge_folder


def get_bridge_name(soup):
    bridge_name = soup.find('h1', class_='center').text.strip()
    return bridge_name


def download_images_by_bridge_type(num_bridges, country_code):
    try:
        bridge_type_url = get_full_bridge_url(country_code, BASE_URL)
    except Exception as e:
        logging.error(f"Error constructing bridge type URL: {e}")
        return

    all_bridge_urls = []
    existing_bridges = set()

    try:
        for bridge_name in os.listdir(IMAGE_FOLDER):
            existing_bridges.add(bridge_name)
    except Exception as e:
        logging.error(f"Error reading bridge folders: {e}")
        return

    while len(all_bridge_urls) < num_bridges:
        try:
            response = requests.get(bridge_type_url)
            soup = BeautifulSoup(response.content, 'html.parser')

            divs = soup.find_all('div', class_='col-md-2')
            for div in divs:
                if div.find('a'):
                    bridge_url = urljoin(BASE_URL, div.find('a')['href'])

                    if bridge_url not in all_bridge_urls and bridge_url not in existing_bridges:
                        all_bridge_urls.append(bridge_url)

            if not all_bridge_urls:
                break
        except Exception as e:
            logging.error(f"An error occurred while fetching bridge links: {e}")
            break

    downloaded_count = 0
    for bridge_url in all_bridge_urls:
        if downloaded_count >= num_bridges:
            break

        try:
            response = requests.get(bridge_url)
            bridge_info_soup = BeautifulSoup(response.content, 'html.parser')
            bridge_name = get_bridge_name(bridge_info_soup)

            if bridge_name in existing_bridges:
                logging.info(f"Folder for bridge {bridge_name} already exists. Skipping...")
                continue

            bridge_info = get_bridge_info(bridge_info_soup)
            logging.info(bridge_info)
            bridge_folder = create_bridge_folder(bridge_name)
            asyncio.run(append_bridge_info_to_csv(bridge_info, summary_csv_path))

            image_data = get_bridge_images(bridge_info_soup)
            if image_data:
                download_images(image_data, bridge_folder)

            downloaded_count += 1
        except Exception as e:
            logging.error(f"An error occurred while processing bridge: {e}")

        time.sleep(time_lag)

    logging.info("All bridges processed!")


def download_images(image_links, bridge_folder):
    if not os.path.exists(bridge_folder):
        os.makedirs(bridge_folder)

    download_images_multithreaded(image_links, bridge_folder)


def download_images_multithreaded(image_links, bridge_folder):
    with concurrent.futures.ThreadPoolExecutor(max_workers=total_workers) as executor:
        futures = []
        for idx, image_link in enumerate(image_links):
            save_path = os.path.join(bridge_folder, f"image_{idx}.jpg")
            futures.append(executor.submit(download_image, image_link, save_path))

        for future in concurrent.futures.as_completed(futures):
            future.result()


def download_image(url, save_path):
    if os.path.exists(save_path):
        logging.error(f"File already exists, skip download: {save_path}")
        return

    try:
        req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(req) as response, open(save_path, 'wb') as out_file:
            data = response.read()
            out_file.write(data)
            logging.info(f"Downloaded {url} to {save_path}")
    except urllib.error.URLError as e:
        logging.error(f"Failed to download image: {url} -> {save_path}, reason: {e}")


def extract_div_data(div):
    div_data = {}
    key = div.find('strong').text.strip() if div.find('strong') else "Unknown"

    texts = [text for text in div.stripped_strings if text != key]
    combined_text = ' '.join(texts).strip()

    div_data[key] = combined_text
    return div_data


def get_bridge_info(soup):
    bridge_info = {}

    bridge_name = soup.find('h1', class_='center').text.strip()
    bridge_info['Bridge Name'] = bridge_name

    info_divs = soup.find_all('div', class_='col-md-3')
    for div in info_divs:
        bridge_info.update(extract_div_data(div))

    info_divs = soup.find_all('div', class_='col-md-2')
    for div in info_divs:
        bridge_info.update(extract_div_data(div))

    return bridge_info


def clean_value(value):
    if isinstance(value, str):
        return value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').strip()
    return value


def get_existing_columns(file_path):
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        headers = next(reader, None)
        if headers:
            headers = [header for header in headers if header != 'Bridge Number']
        return headers if headers else []


async def append_bridge_info_to_csv(bridge_info, file_path):
    folder_path = os.path.dirname(file_path)
    create_folder(folder_path)

    existing_columns = get_existing_columns(file_path)
    all_columns = ['Bridge Number'] + list(existing_columns) + [col for col in bridge_info.keys() if
                                                                col not in existing_columns]

    bridge_number = sum(1 for row in open(file_path, 'r', encoding='utf-8')) if os.path.exists(file_path) else 1

    if not existing_columns:
        async with aiofiles.open(file_path, 'w', newline='', encoding='utf-8') as f:
            await f.write(';'.join(all_columns) + '\n')

    elif len(all_columns) > len(existing_columns) + 1:
        temp_data = []
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader((await f.read()).splitlines(), delimiter=';')
            next(reader, None)
            for row in reader:
                while len(row) < len(existing_columns) + 1:
                    row.append("N/A")
                temp_data.append(row)

        async with aiofiles.open(file_path, 'w', newline='', encoding='utf-8') as f:
            await f.write(';'.join(all_columns) + '\n')
            for row in temp_data:
                row.extend("N/A" for _ in range(len(all_columns) - len(row)))
                await f.write(';'.join(row) + '\n')

    cleaned_bridge_info = {key: clean_value(value) for key, value in bridge_info.items()}
    bridge_data = [bridge_number] + [cleaned_bridge_info.get(column, "N/A") for column in all_columns[1:]]

    async with aiofiles.open(file_path, 'a', newline='', encoding='utf-8') as f:
        await f.write(';'.join(map(str, bridge_data)) + '\n')


def log_runtime(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"{func.__name__} runtime：{end_time - start_time} s")
        return result

    return wrapper


@log_runtime
def main():
    path = IMAGE_FOLDER + '/'
    create_folder(path)

    try:
        list_supported_countries()
        country_code = input(
            "What country are you looking for a bridge to?: ").strip().upper()

        try:
            num_bridges = int(input("How many bridges do you want to download? "))
        except ValueError:
            print("Invalid number. Exiting.")
            logging.error("Invalid number. Exiting.")
            return

        download_images_by_bridge_type(num_bridges, country_code)
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
    input("Press the Enter key to exit the program...")
