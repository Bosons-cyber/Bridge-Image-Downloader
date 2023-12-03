import csv
import os
import re
import shutil

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import concurrent.futures
import time
import urllib.request
import urllib.parse
import urllib.error
from bs4 import BeautifulSoup
import ssl
import requests
import logging
import json
import asyncio
import aiofiles
from logging.handlers import RotatingFileHandler
from requests.exceptions import RequestException
from requests import Session

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

BASE_URL = config['BASE_URL']
USER_AGENT = config['USER_AGENT']
IMAGE_FOLDER = config['IMAGE_FOLDER']
WINDOW_SIZE_WIDTH = config['WINDOW_SIZE_WIDTH']
WINDOW_SIZE_HEIGHT = config['WINDOW_SIZE_HEIGHT']
output_folder = config['output_folder']
time_lag = config['time_lag']
total_workers = config['total_workers']
chrome_driver_path = config['chrome_driver_path']
template_folder = config['template_folder']
language = config['language']

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler('bridge_downloader.log', maxBytes=10000, backupCount=5)
    ]
)


def navigate_and_wait(driver, url):
    driver.set_window_size(WINDOW_SIZE_WIDTH, WINDOW_SIZE_HEIGHT)
    driver.get(url)
    return BeautifulSoup(driver.page_source, 'html.parser')


def get_full_bridge_url(country_code, bridge_type, base_usl):
    if country_code:
        final_address = f"{base_usl}/bauwerke/bruecken/{bridge_type}/liste?filtercountry={country_code}"
        return final_address
    else:
        final_address = f"{base_usl}/bauwerke/bruecken/{bridge_type}/liste"
        return final_address


def get_bridge_media_soup(driver, url):
    media_url = f"{url}/medien"
    driver.set_window_size(WINDOW_SIZE_WIDTH, WINDOW_SIZE_HEIGHT)
    driver.get(media_url)

    try:
        WebDriverWait(driver, 10).until(
            ec.presence_of_element_located((By.CSS_SELECTOR, "a.imageThumbLink_2"))
        )
    except TimeoutException:
        logging.info("No image found for the bridge. Continuing to extract other information...")
        print("No image found for the bridge. Continuing to extract other information...")

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    return soup


def choose_bridge_type():
    try:
        with open("bridge_types.json", "r", encoding="utf-8") as file:
            bridge_types = json.load(file)
            print("Available bridge types:\n")
            for code, name in bridge_types.items():
                print(f"{code}: {name}")
    except FileNotFoundError:
        print("Bridge_types file not found.")
        return
    except json.JSONDecodeError:
        print("Error decoding JSON from the country codes file.")
        return
    except Exception as e:
        print(f"Error reading country codes file: {e}")
        return

    text = input("Please enter the name of the bridge type: ")
    typename = format_text(text)

    return typename


def list_supported_countries():
    try:
        with open("country_codes.json", "r", encoding="utf-8") as file:
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


def choose_search_type():
    bridge_search_types = {
        "Name": "name",
        "Type": "type",
    }
    print("Please choose a search type:")
    for idx, (name, _) in enumerate(bridge_search_types.items(), 1):
        print(f"{idx}. {name}")
    choice = int(input("Enter the number of your choice: "))
    chosen_type = list(bridge_search_types.values())[choice - 1]
    return chosen_type


def clean_folder_name(folder_name):
    return re.sub(r'[<>:"/\\|?*]', '_', folder_name)


def create_folder(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


def create_unique_bridge_folder_from_url(bridge_url):
    bridge_folder = os.path.join(IMAGE_FOLDER, get_unique_bridge_name_from_url(bridge_url))
    create_folder(bridge_folder)

    return bridge_folder


def get_unique_bridge_name_from_url(bridge_url):
    unique_identifier = bridge_url.rstrip('/').split('/')[-1]
    return unique_identifier


def download_images_by_bridge_name(driver, bridge_names, base_url):
    problematic_bridges = []
    session = Session()

    for bridge_name_to_download in bridge_names:
        print(f"Processing bridge: {bridge_name_to_download}")
        logging.info(f"Processing bridge: {bridge_name_to_download}")
        formatted_bridge_name = format_text(bridge_name_to_download)

        bridge_url_de = f"{base_url}/bauwerke/{formatted_bridge_name}"
        bridge_info_soup_de = navigate_and_wait(driver, bridge_url_de)
        bridge_url_en = get_en_link(bridge_info_soup_de)
        if language == "English":
            bridge_url = BASE_URL + bridge_url_en
            bridge_info_soup = navigate_and_wait(driver, bridge_url)
        else:
            bridge_url = bridge_url_de
            bridge_info_soup = bridge_info_soup_de

        try:
            try:
                response = requests.get(bridge_url)

                if response.status_code != 200:
                    raise RequestException(f"Failed to fetch bridge data: {response.status_code}")

            except RequestException as e:
                logging.warning(f"Bridge not found or network error: {e}")
                problematic_bridges.append(bridge_name_to_download)
                continue

            bridge_info = get_bridge_info(bridge_info_soup)

            bridge_folder = create_unique_bridge_folder_from_url(bridge_url_de)

            asyncio.run(process_all_templates(bridge_info))

            bridge_media_soup = get_bridge_media_soup(driver, bridge_url_de)
            image_data = get_image_data(bridge_media_soup)
            if not image_data:
                problematic_bridges.append(bridge_name_to_download)
            else:
                download_images(image_data, bridge_folder)
            time.sleep(1)

        except Exception as e:
            logging.error(f"An error occurred while processing {bridge_name_to_download}: {e}")
            problematic_bridges.append(bridge_name_to_download)

    print(f"Problematic bridges : {problematic_bridges}")
    logging.info(f"Problematic bridges : {problematic_bridges}")

    session.close()


def download_images_by_bridge_type(driver, bridge_type, num_bridges, base_url, country_code=None):
    try:
        bridge_type_url = get_full_bridge_url(country_code, bridge_type, base_url)
    except Exception as e:
        print(f"Error constructing bridge type URL: {e}")
        logging.error(f"Error constructing bridge type URL: {e}")
        return

    all_bridge_urls = []
    page = 0
    existing_bridges = set()

    try:
        for bridge_name in os.listdir(IMAGE_FOLDER):
            existing_bridges.add(bridge_name)
    except Exception as e:
        print(f"Error constructing bridge type URL: {e}")
        logging.error(f"Error reading bridge folders: {e}")
        return

    while len(all_bridge_urls) < num_bridges:
        try:
            current_page_url = bridge_type_url if page == 0 else f"{bridge_type_url}?min={page * 100}"
            navigate_and_wait(driver, current_page_url)

            WebDriverWait(driver, 10).until(
                ec.presence_of_element_located((By.CSS_SELECTOR, "td > a.listableleft"))
            )
            bridge_links = driver.find_elements(By.CSS_SELECTOR, "td > a.listableleft")
        except TimeoutException:
            logging.info("Timed out waiting for page to load")
            break
        except NoSuchElementException:
            logging.info("No bridge links found on the page")
            break
        except Exception as e:
            logging.error(f"An error occurred while fetching bridge links: {e}")
            break

        if not bridge_links:
            break

        bridge_urls = [link.get_attribute("href") for link in bridge_links]
        all_bridge_urls.extend(bridge_urls)

        page += 1

    downloaded_count = 0
    for idx, bridge_url_de in enumerate(all_bridge_urls, 1):
        bridge_info_soup_de = navigate_and_wait(driver, bridge_url_de)
        bridge_url_en = get_en_link(bridge_info_soup_de)
        if language == "English":
            bridge_url = BASE_URL + bridge_url_en
            bridge_info_soup = navigate_and_wait(driver, bridge_url)
        else:
            bridge_info_soup = bridge_info_soup_de

        bridge_unique_name = get_unique_bridge_name_from_url(bridge_url_de)

        if bridge_unique_name in existing_bridges:
            logging.info(f"Folder for bridge {bridge_unique_name} already exists. Skipping...")
            continue
        try:
            bridge_info = get_bridge_info(bridge_info_soup)

            logging.info(f"Processing bridge {downloaded_count + 1} of {num_bridges}...")
            print(f"Processing bridge {downloaded_count + 1} of {num_bridges}...")

            bridge_folder = create_unique_bridge_folder_from_url(bridge_url_de)
            asyncio.run(process_all_templates(bridge_info))

            bridge_media_soup = get_bridge_media_soup(driver, bridge_url_de)
            image_data = get_image_data(bridge_media_soup)

            if image_data:
                download_images(image_data, bridge_folder)
        except RequestException as e:
            logging.error(f"Network error while processing bridge: {e}")
        except Exception as e:
            logging.error(f"An error occurred while processing bridge: {e}")

        time.sleep(time_lag)

        downloaded_count += 1
        if downloaded_count >= num_bridges:
            break

    logging.info("All bridges processed!")
    print("All bridges processed!")


def get_image_data(soup):
    image_entries = soup.find_all('div', class_='jg-entry')

    image_data = []
    for entry in image_entries:
        link = entry.find('a', class_='imageThumbLink_2')
        if link:
            href = link['href']
            image_data.append(href)

    return image_data


def get_download_link(soup):
    img_tag = soup.find('img', {'class': 'flexible bordered mediaObject'})
    if img_tag:
        return img_tag['src']

    return None


def get_en_link(soup):
    li_tag = soup.select_one('li.short-language:not(.language-active-li)')
    if li_tag:
        a_tag = li_tag.find('a')
        if a_tag:
            return a_tag['href']

    return None


def download_images(image_data, bridge_folder):
    high_res_image_links = []
    for high_res_image_url in image_data:
        response = requests.get(BASE_URL + high_res_image_url)
        new_soup = BeautifulSoup(response.content, 'html.parser')
        download_link = get_download_link(new_soup)
        if download_link:
            high_res_image_links.append(download_link)

    download_images_multithreaded(high_res_image_links, bridge_folder)


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


def download_images_multithreaded(image_links, bridge_folder):
    global total_workers

    with concurrent.futures.ThreadPoolExecutor(max_workers=total_workers) as executor:
        futures = []
        for idx, image_link in enumerate(image_links):
            save_path = os.path.join(bridge_folder, f"image_{idx}.jpg")
            futures.append(executor.submit(download_image, image_link, save_path))

        for future in concurrent.futures.as_completed(futures):
            future.result()


def format_text(text):
    name = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss").replace("Ä", "AE").replace(
        "Ö", "Oe").replace("Ü", "Ue")
    name = name.replace(" ", "-").lower()
    return name


def extract_table_data(table):
    data = {}
    rows = table.find_all('tr')
    for row in rows:
        header = row.find('th')
        value = row.find('td')
        if header and value:
            data[header.text.strip()] = value.text.strip()
    return data


def extract_technical_data(technical_div, bridge_info):
    tab_bodies = technical_div.find_all('div', class_='tabbody')

    for tab_body in tab_bodies:
        category_prefix = tab_body.find_previous('h3').text.strip()
        table = tab_body.find('table')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['th', 'td'])
                if len(cells) > 1:
                    if cells[0].name == 'td' and not cells[0].text.strip():
                        header = cells[1].text.strip()
                        value = cells[2].text.strip() if len(cells) > 2 else ""
                    else:
                        header = cells[0].text.strip()
                        value = cells[1].text.strip()
                    full_key = f"{category_prefix} - {header}"
                    bridge_info[full_key] = value


def get_bridge_info(soup):
    bridge_info = {}

    table_ids = ['general', 'typology', 'geographic']
    for table_id in table_ids:
        table = soup.find('div', {'class': 'js-acordion-body', 'id': table_id}).find('table',
                                                                                     {'class': 'aligned-tables'})
        bridge_info.update(extract_table_data(table))

    technical_info_div = soup.find('div', {'class': 'js-acordion-body', 'id': 'technical'})
    if technical_info_div:
        extract_technical_data(technical_info_div, bridge_info)
    else:
        logging.error("Technical information is not available.")

    bridge_name_tag = soup.find("h1", {"itemprop": "name"})
    bridge_info["Bridge Name"] = bridge_name_tag.get_text(strip=True) if bridge_name_tag else "Unknown_bridge"

    return bridge_info


def clean_value(value):
    if isinstance(value, str):
        return value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').strip()
    return value


def get_template_columns(file_path):
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        headers = next(reader, None)
        if headers:
            headers = [header for header in headers if header != '\ufeffbridge_id:']
        return headers if headers else []


async def append_bridge_info_to_csv(bridge_info, template_path, output_path):
    folder_path = os.path.dirname(output_path)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    template_columns = get_template_columns(template_path)
    print(template_columns)

    cleaned_bridge_info = {key: clean_value(value) for key, value in bridge_info.items()}
    bridge_number = await get_next_bridge_number(output_path)
    bridge_data = [bridge_number] + [cleaned_bridge_info.get(column, "N/A") for column in template_columns]

    # 写入数据
    async with aiofiles.open(output_path, 'a', newline='', encoding='utf-8') as f:
        await f.write('\n' + ';'.join(map(str, bridge_data)))


async def get_next_bridge_number(output_path):
    bridge_number = 0
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        async with aiofiles.open(output_path, 'r', encoding='utf-8') as f:
            last_line = await get_last_line(f)
            if last_line:  # 确保最后一行不是空的
                # 假设编号是第一列
                last_number = last_line.split(';')[0]
                if last_number.isdigit():
                    bridge_number = int(last_number)
    bridge_number += 1
    return bridge_number  # 直接返回数字编号


async def get_last_line(f):
    last_line = ''
    while True:
        line = await f.readline()
        if not line:  # 当读到文件末尾时，line 会是空字符串
            break
        last_line = line
    return last_line.strip()


async def process_all_templates(bridge_info):
    # 获取所有模板文件的路径
    template_files = [f for f in os.listdir(template_folder) if f.endswith('.csv')]

    # 遍历并处理每个模板文件
    for template_file in template_files:
        template_path = os.path.join(template_folder, template_file)
        output_path = os.path.join(output_folder, template_file)
        await append_bridge_info_to_csv(bridge_info, template_path, output_path)


async def copy_all_templates():
    template_files = [f for f in os.listdir(template_folder) if f.endswith('.csv')]

    for template_file in template_files:
        template_path = os.path.join(template_folder, template_file)
        output_path = os.path.join(output_folder, template_file)  # 不更改文件名
        shutil.copyfile(template_path, output_path)  # 直接复制文件


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
    base_url_suffix = '/de'
    base_url = BASE_URL + base_url_suffix

    driver = None
    try:
        options = webdriver.ChromeOptions()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Chrome(
            service=Service(executable_path=chrome_driver_path),
            options=options
        )
    except Exception as e:
        print(f"Error initializing the WebDriver: {e}")
        logging.error(f"Error initializing the WebDriver: {e}")
        return

    create_folder(IMAGE_FOLDER)
    create_folder(output_folder)
    asyncio.run(copy_all_templates())

    login_choice = input("Would you like to log in? (y/n): ").lower()

    if login_choice == 'y':
        driver.set_window_size(WINDOW_SIZE_WIDTH, WINDOW_SIZE_HEIGHT)
        driver.get(base_url)
        try:
            login_button = driver.find_element(By.ID, "myStructuraeLoginBtn")
            if login_button:
                input("Please log in manually in your browser and press Enter to continue...")
        except Exception as e:
            print(f"Error finding the login button: {e}")
            logging.error(f"Error finding the login button: {e}")

    chosen_type = choose_search_type()

    try:
        if chosen_type == 'name':
            with open("bridges.txt", "r", encoding="utf-8") as file:
                bridge_names = [line.strip() for line in file]
            download_images_by_bridge_name(driver, bridge_names, base_url)
        elif chosen_type == 'type':
            bridge_type = choose_bridge_type()
            try:
                num_bridges = int(input("How many bridges do you want to download? "))
            except ValueError:
                print("Invalid number. Exiting.")
                logging.error("Invalid number. Exiting.")
                return

            if login_choice == 'y':
                country_mode = input("Do you want to search by country?(y/n): ").lower()
            else:
                country_mode = "n"

            if country_mode == "y":
                list_supported_countries()
                country_code = input(
                    "Please enter the country code: ").strip().upper()
                download_images_by_bridge_type(driver, bridge_type, num_bridges, base_url, country_code)
            else:
                download_images_by_bridge_type(driver, bridge_type, num_bridges, base_url)
        else:
            print("Invalid mode selected. Exiting.")
            logging.error("Invalid mode selected. Exiting.")
            return
    except FileNotFoundError:
        print("Please create a bridge.txt and add the name of the bridge to be processed line by line in it.")
        logging.error("Please create a bridge.txt and add the name of the bridge to be processed line by line in it.")
    except Exception as e:
        print(f"An error occurred: {e}")
        logging.error(f"An error occurred: {e}")
    finally:
        if driver:
            driver.quit()


if __name__ == "__main__":
    again = "y"
    while again == "y":
        main()
        again = input("Do you want to do it again?(y/n)")
