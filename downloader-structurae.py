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

# Bypass SSL certificate verification for HTTPS requests
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# Load configuration from 'config.json'
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

# Configuration variables
base_URL = config['base_URL']
user_agent = config['user_agent']
image_folder = config['image_folder']
window_size_width = config['window_size_width']
window_size_height = config['window_size_height']
output_folder = config['output_folder']
summary_csv_path = config['summary_csv_path']
time_lag = config['time_lag']
download_timeout = config['download_timeout']
threat_timeout = config['threat_timeout']
multithreading = config['multithreading']
total_workers = config['total_workers']
chrome_driver_path = config['chrome_driver_path']
template_folder_en = config['template_folder_en']
template_folder_de = config['template_folder_de']
language = config['language']

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler('bridge_downloader.log', maxBytes=2000, backupCount=5, encoding='utf-8')
    ]
)


def navigate_and_wait(driver, url):
    """
        Navigates to a given URL using the Selenium WebDriver and waits until the page is fully loaded.
        Args:
            driver: Selenium WebDriver instance.
            url: URL to navigate to.
        Returns:
            BeautifulSoup object of the page source after loading.
        """
    driver.set_window_size(window_size_width, window_size_height)
    driver.get(url)
    return BeautifulSoup(driver.page_source, 'html.parser')


def get_full_bridge_url(country_code, bridge_type, base_usl):
    """
        Constructs the full URL for a specific bridge type and country.
        Args:
            country_code: Code of the country.
            bridge_type: Type of the bridge.
            base_usl: Base URL for the website.
        Returns:
            The constructed URL as a string.
        """
    if country_code:
        final_address = f"{base_usl}/bauwerke/bruecken/{bridge_type}/liste?filtercountry={country_code}"
        return final_address
    else:
        final_address = f"{base_usl}/bauwerke/bruecken/{bridge_type}/liste"
        return final_address


def get_bridge_media_soup(driver, url):
    """
        Navigates to the media page of a bridge and returns its BeautifulSoup object.
        Args:
            driver: Selenium WebDriver instance.
            url: URL of the bridge's main page.
        Returns:
            BeautifulSoup object of the bridge's media page.
        """
    media_url = f"{url}/medien"
    driver.set_window_size(window_size_width, window_size_height)
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
    """
        Prompts the user to choose a bridge type from a list loaded from a JSON file.
        Returns:
            The chosen bridge type as a string.
        """
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
    """
        Lists supported countries by reading from a JSON file.
        """
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
    """
        Prompts the user to choose a search type (by name or type).
        Returns:
            The chosen search type as a string.
        """
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
    """
        Cleans and formats the folder name by removing invalid characters.
        Args:
            folder_name: The original folder name.
        Returns:
            Cleaned folder name as a string.
        """
    return re.sub(r'[<>:"/\\|?*]', '_', folder_name)


def create_folder(folder_name):
    """
        Creates a new folder if it does not already exist.
        Args:
            folder_name: Name of the folder to create.
        """
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


def create_unique_bridge_folder_from_url(bridge_url):
    """
        Creates a unique folder for a bridge based on its URL.
        Args:
            bridge_url: URL of the bridge.
        Returns:
            Path of the created folder.
        """
    bridge_folder = os.path.join(image_folder, get_unique_bridge_name_from_url(bridge_url))
    create_folder(bridge_folder)

    return bridge_folder


def get_unique_bridge_name_from_url(bridge_url):
    """
        Extracts a unique bridge name from its URL.
        Args:
            bridge_url: URL of the bridge.
        Returns:
            Unique bridge name as a string.
        """
    unique_identifier = bridge_url.rstrip('/').split('/')[-1]
    return unique_identifier


def download_images_by_bridge_name(driver, bridge_names, base_url, key_mapping):
    """
        Downloads images for each bridge specified by name.
        Args:
            driver: Selenium WebDriver instance.
            bridge_names: List of bridge names.
            base_url: Base URL of the website.
            key_mapping: Mapping of keys for data extraction.
        """
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
            bridge_url = base_url + bridge_url_en
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
            cleaned_bridge_info = {clean_value(key): clean_value(value) for key, value in bridge_info.items()}
            replaced_bridge_info = replace_keys_in_dict(cleaned_bridge_info, key_mapping)

            bridge_folder = create_unique_bridge_folder_from_url(bridge_url_de)

            bridge_media_soup = get_bridge_media_soup(driver, bridge_url_de)
            image_data = get_image_data(bridge_media_soup)
            if not image_data:
                image_count = 0
                problematic_bridges.append(bridge_name_to_download)
            else:
                image_count = len(image_data)
                download_images(image_data, bridge_folder)

            if language == "English":
                replaced_bridge_info['Image Count'] = image_count
            else:
                replaced_bridge_info['Anzahl der Bilder'] = image_count

            if language == "English":
                replaced_bridge_info['Unique Name'] = get_unique_bridge_name_from_url(bridge_url_de)
            else:
                replaced_bridge_info['eindeutiger Name'] = get_unique_bridge_name_from_url(bridge_url_de)

            asyncio.run(process_all_templates(replaced_bridge_info))
            asyncio.run(append_bridge_info_to_summary(replaced_bridge_info, summary_csv_path))

            time.sleep(1)

        except Exception as e:
            logging.error(f"An error occurred while processing {bridge_name_to_download}: {e}")
            problematic_bridges.append(bridge_name_to_download)

    print(f"Problematic bridges : {problematic_bridges}")
    logging.info(f"Problematic bridges : {problematic_bridges}")

    session.close()


def download_images_by_bridge_type(driver, bridge_type, num_bridges, base_url, key_mapping, country_code=None):
    """
        Downloads images for bridges of a specific type.
        Args:
            driver: Selenium WebDriver instance.
            bridge_type: Type of the bridge.
            num_bridges: Number of bridges to download.
            base_url: Base URL of the website.
            key_mapping: Mapping of keys for data extraction.
            country_code: Optional country code.
        """
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
        for bridge_name in os.listdir(image_folder):
            existing_bridges.add(bridge_name)
    except Exception as e:
        print(f"Error constructing bridge type URL: {e}")
        logging.error(f"Error reading bridge folders: {e}")
        return

    while len(all_bridge_urls) < num_bridges:
        unprocessed_urls = []

        try:
            current_page_url = bridge_type_url if page == 0 else f"{bridge_type_url}?min={page * 100}"
            navigate_and_wait(driver, current_page_url)

            WebDriverWait(driver, 60).until(
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

        for url in bridge_urls:
            bridge_unique_name = get_unique_bridge_name_from_url(url)
            if bridge_unique_name not in existing_bridges:
                unprocessed_urls.append(url)
            else:
                logging.info(f"Folder for bridge {bridge_unique_name} already exists. Skipping...")

        all_bridge_urls.extend(unprocessed_urls)

        page += 1

    downloaded_count = 0
    for idx, bridge_url_de in enumerate(all_bridge_urls, 1):
        bridge_info_soup_de = navigate_and_wait(driver, bridge_url_de)

        bridge_url_en = get_en_link(bridge_info_soup_de)
        if language == "English":
            bridge_url = base_url + bridge_url_en
            bridge_info_soup = navigate_and_wait(driver, bridge_url)
        else:
            bridge_info_soup = bridge_info_soup_de
        try:
            bridge_info = get_bridge_info(bridge_info_soup)
            cleaned_bridge_info = {clean_value(key): clean_value(value) for key, value in bridge_info.items()}
            replaced_bridge_info = replace_keys_in_dict(cleaned_bridge_info, key_mapping)

            logging.info(f"Processing bridge {downloaded_count + 1} of {num_bridges}...")
            print(f"Processing bridge {downloaded_count + 1} of {num_bridges}...")

            bridge_folder = create_unique_bridge_folder_from_url(bridge_url_de)

            bridge_media_soup = get_bridge_media_soup(driver, bridge_url_de)
            image_data = get_image_data(bridge_media_soup)

            if image_data:
                image_count = len(image_data)
                download_images(image_data, bridge_folder)
            else:
                image_count = 0

            if language == "English":
                replaced_bridge_info['Image Count'] = image_count
            else:
                replaced_bridge_info['Anzahl der Bilder'] = image_count

            if language == "English":
                replaced_bridge_info['Unique Name'] = get_unique_bridge_name_from_url(bridge_url_de)
            else:
                replaced_bridge_info['eindeutiger Name'] = get_unique_bridge_name_from_url(bridge_url_de)

            asyncio.run(process_all_templates(replaced_bridge_info))
            asyncio.run(append_bridge_info_to_summary(replaced_bridge_info, summary_csv_path))

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
    """
        Extracts image data from the BeautifulSoup object of a bridge's media page.
        Args:
            soup: BeautifulSoup object of the bridge's media page.
        Returns:
            A list of image URLs.
        """
    image_entries = soup.find_all('div', class_='jg-entry')

    image_data = []
    for entry in image_entries:
        link = entry.find('a', class_='imageThumbLink_2')
        if link:
            href = link['href']
            image_data.append(href)

    return image_data


def get_download_link(soup):
    """
        Extracts the download link for an image from its BeautifulSoup object.
        Args:
            soup: BeautifulSoup object of the image page.
        Returns:
            The download link for the image.
        """
    img_tag = soup.find('img', {'class': 'flexible bordered mediaObject'})
    if img_tag:
        return img_tag['src']

    return None


def get_en_link(soup):
    """
        Extracts the English version link of a bridge page from its BeautifulSoup object.
        Args:
            soup: BeautifulSoup object of the bridge's page.
        Returns:
            The URL of the English version of the page.
        """
    li_tag = soup.select_one('li.short-language:not(.language-active-li)')
    if li_tag:
        a_tag = li_tag.find('a')
        if a_tag:
            return a_tag['href']

    return None


def download_images(image_data, bridge_folder):
    """
        Downloads images from the provided list of image URLs into the specified folder.
        Args:
            image_data: List of image URLs.
            bridge_folder: Folder path where images will be saved.
        """
    high_res_image_links = []
    for high_res_image_url in image_data:
        response = requests.get(base_URL + high_res_image_url)
        new_soup = BeautifulSoup(response.content, 'html.parser')
        download_link = get_download_link(new_soup)
        if download_link:
            high_res_image_links.append(download_link)

    if multithreading == "True":
        download_images_multithreaded(high_res_image_links, bridge_folder)
    else:
        for idx, image_link in enumerate(high_res_image_links):
            save_path = os.path.join(bridge_folder, f"image_{idx}.jpg")
            download_image(image_link, save_path)


def download_image(url, save_path):
    """
        Downloads a single image from the given URL and saves it to the specified path.
        Args:
            url: URL of the image to download.
            save_path: Path where the image will be saved.
        """
    if os.path.exists(save_path):
        logging.error(f"File already exists, skip download: {save_path}")
        return

    try:
        req = urllib.request.Request(url, headers={'User-Agent': user_agent})
        with urllib.request.urlopen(req, timeout=download_timeout) as response, open(save_path, 'wb') as out_file:
            data = response.read()
            out_file.write(data)
            logging.info(f"Downloaded {url} to {save_path}")
    except urllib.error.URLError as e:
        logging.error(f"Failed to download image: {url} -> {save_path}, reason: {e}")
    except Exception as e:
        logging.error(f"Error downloading image: {url} -> {save_path}, reason: {e}")


def download_images_multithreaded(image_links, bridge_folder):
    """
        Downloads multiple images in parallel using multithreading.
        Args:
            image_links: List of image URLs to download.
            bridge_folder: Folder path where images will be saved.
        """
    global total_workers

    with concurrent.futures.ThreadPoolExecutor(max_workers=total_workers) as executor:
        futures = []
        for idx, image_link in enumerate(image_links):
            save_path = os.path.join(bridge_folder, f"image_{idx}.jpg")
            futures.append(executor.submit(download_image, image_link, save_path))

        for future in concurrent.futures.as_completed(futures, timeout=threat_timeout):
            try:
                future.result()
            except concurrent.futures.TimeoutError:
                logging.error("A download thread has timed out and been skipped.")


def format_text(text):
    """
       Formats the given text by replacing special characters and converting to lowercase.
       Args:
           text: The text to format.
       Returns:
           Formatted text as a string.
       """
    name = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss").replace("Ä", "AE").replace(
        "Ö", "Oe").replace("Ü", "Ue")
    name = name.replace(" ", "-").lower()
    return name


def extract_table_data(table):
    """
        Extracts data from a table element in the BeautifulSoup object.
        Args:
            table: Table element from BeautifulSoup object.
        Returns:
            Dictionary of extracted data with headers as keys and corresponding values.
        """
    data = {}
    rows = table.find_all('tr')
    for row in rows:
        header = row.find('th')
        value = row.find('td')
        if header and value:
            data[header.text.strip()] = value.text.strip()
    return data


def extract_technical_data(technical_div, bridge_info):
    """
        Extracts technical data about a bridge from its BeautifulSoup object.
        Args:
            technical_div: Div element containing technical data from BeautifulSoup object.
            bridge_info: Dictionary to store extracted data.
        """
    tab_bodies = technical_div.find_all('div', class_='tabbody')

    for tab_body in tab_bodies:
        table = tab_body.find('table')
        if table:
            rows = table.find_all('tr')
            current_header = ""

            for row in rows:
                cells = row.find_all(['th', 'td'])

                if len(cells) == 3:
                    current_header = cells[0].text.strip()
                    key = cells[1].text.strip()
                    value = cells[2].text.strip()
                    full_key = f"{current_header} {key}" if current_header else key
                elif len(cells) == 2:
                    if 'rowspan' in cells[0].attrs:
                        current_header = cells[0].text.strip()
                        key = cells[1].text.strip()
                        full_key = current_header
                    else:
                        key = cells[0].text.strip()
                        value = cells[1].text.strip()
                        full_key = f"{current_header} {key}" if current_header else key
                elif len(cells) == 1:
                    value = cells[0].text.strip()
                    full_key = current_header

                bridge_info[full_key] = value


def get_bridge_info(soup):
    """
        Extracts comprehensive information about a bridge from its BeautifulSoup object.
        Args:
            soup: BeautifulSoup object of the bridge's page.
        Returns:
            Dictionary containing various details about the bridge.
        """
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
    if language == "English":
        bridge_info["Bridge Name"] = bridge_name_tag.get_text(strip=True) if bridge_name_tag else "Unknown_bridge"
    else:
        bridge_info["Brücke Name"] = bridge_name_tag.get_text(strip=True) if bridge_name_tag else "Unbekannte_Brücke"

    return bridge_info


def replace_keys_in_dict(original_dict, key_mapping):
    """
        Replaces keys in a dictionary based on a provided mapping.
        Args:
            original_dict: The original dictionary with keys to be replaced.
            key_mapping: Dictionary mapping old keys to new keys.
        Returns:
            New dictionary with keys replaced as per the mapping.
        """
    new_dict = {}
    for key, value in original_dict.items():
        new_key = key_mapping.get(key, key)
        new_dict[new_key] = value
    return new_dict


def clean_value(value):
    """
        Cleans a given value by removing unwanted characters and whitespace.
        Args:
            value: The value to be cleaned.
        Returns:
            Cleaned value as a string.
        """
    if isinstance(value, str):
        return value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace(':', '').strip()
    return value


def get_existing_columns(file_path):
    """
        Retrieves existing column headers from a CSV file.
        Args:
            file_path: Path to the CSV file.
        Returns:
            A list of existing column headers, excluding 'Bridge Number' and 'Brückennummer'.
        """
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        headers = next(reader, None)
        if headers:
            # Filter out specific headers
            headers = [header for header in headers if header not in ['Bridge Number', 'Brückennummer']]
        return headers if headers else []


async def append_bridge_info_to_summary(bridge_info, file_path):
    """
        Asynchronously appends bridge information to a summary CSV file.
        Args:
            bridge_info: Dictionary containing bridge information.
            file_path: Path to the summary CSV file.
        """
    # Create the folder for the file if it doesn't exist
    folder_path = os.path.dirname(file_path)
    create_folder(folder_path)

    # Get existing columns and determine all columns to be included
    existing_columns = get_existing_columns(file_path)
    if language == "English":
        all_columns = ['Bridge Number'] + list(existing_columns) + [col for col in bridge_info.keys() if
                                                                    col not in existing_columns]
    else:
        all_columns = ['Brückennummer'] + list(existing_columns) + [col for col in bridge_info.keys() if
                                                                    col not in existing_columns]

    # Calculate the bridge number
    bridge_number = sum(1 for row in open(file_path, 'r', encoding='utf-8')) if os.path.exists(file_path) else 1

    # Write headers if the file is new or update the file if new columns are added
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

    # Clean the bridge information and prepare the data for writing
    cleaned_bridge_info = {key: clean_value(value) for key, value in bridge_info.items()}
    bridge_data = [bridge_number] + [cleaned_bridge_info.get(column, "N/A") for column in all_columns[1:]]

    # Append the bridge data to the file
    async with aiofiles.open(file_path, 'a', newline='', encoding='utf-8') as f:
        await f.write(';'.join(map(str, bridge_data)) + '\n')


def get_template_columns(file_path):
    """
        Retrieves column headers from a CSV template file.
        Args:
            file_path: Path to the CSV template file.
        Returns:
            A list of column headers from the CSV file.
        """
    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        return []
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f, delimiter=';')
        headers = next(reader, None)
        if headers:
            headers = [header for header in headers if header != 'bridge_id' and header != 'Brücke_id']
        return headers if headers else []


async def append_bridge_info_to_csv(bridge_info, template_path, output_path):
    """
        Asynchronously appends bridge information to a CSV file based on a template.
        Args:
            bridge_info: Dictionary containing bridge information.
            template_path: Path to the CSV template file.
            output_path: Path to the output CSV file where data will be appended.
        """
    folder_path = os.path.dirname(output_path)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    template_columns = [col.lower() for col in get_template_columns(template_path)]

    bridge_info_lower = {key.lower(): value for key, value in bridge_info.items()}
    bridge_number = await get_next_bridge_number(output_path)

    bridge_data = [bridge_number] + [bridge_info_lower.get(column, "N/A") for column in template_columns]

    async with aiofiles.open(output_path, 'a', encoding='utf-8') as f:
        await f.write('\n' + ';'.join(map(str, bridge_data)))


async def get_next_bridge_number(output_path):
    """
        Asynchronously retrieves the next bridge number to be used in the output CSV file.
        Args:
            output_path: Path to the output CSV file.
        Returns:
            The next bridge number as an integer.
        """
    bridge_number = 0
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        async with aiofiles.open(output_path, 'r', encoding='utf-8') as f:
            last_line = await get_last_line(f)
            if last_line:
                last_number = last_line.split(';')[0]
                if last_number.isdigit():
                    bridge_number = int(last_number)
    bridge_number += 1
    return bridge_number


async def get_last_line(f):
    """
        Asynchronously reads the last line of a file.
        Args:
            f: File object opened for reading.
        Returns:
            The last line of the file as a string.
        """
    last_line = ''
    while True:
        line = await f.readline()
        if not line:
            break
        last_line = line
    return last_line.strip()


async def process_all_templates(bridge_info):
    """
        Asynchronously processes all CSV templates and appends bridge information to them.
        Args:
            bridge_info: Dictionary containing bridge information.
        """
    if language == "English":
        template_folder = template_folder_en
    else:
        template_folder = template_folder_de

    template_files = [f for f in os.listdir(template_folder) if f.endswith('.csv')]

    for template_file in template_files:
        template_path = os.path.join(template_folder, template_file)
        output_path = os.path.join(output_folder, template_file)
        await append_bridge_info_to_csv(bridge_info, template_path, output_path)


def copy_all_templates():
    """
        Copies all CSV template files from the template folder to the output folder.
        """
    if language == "English":
        template_folder = template_folder_en
    else:
        template_folder = template_folder_de

    template_files = [f for f in os.listdir(template_folder) if f.endswith('.csv')]

    for template_file in template_files:
        template_path = os.path.join(template_folder, template_file)
        output_path = os.path.join(output_folder, template_file)  # 不更改文件名
        shutil.copyfile(template_path, output_path)  # 直接复制文件


def log_runtime(func):
    """
        Decorator function to log the runtime of a function.
        Args:
            func: The function whose runtime is to be logged.
        Returns:
            Wrapper function that logs the runtime.
        """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        logging.info(f"{func.__name__} runtime：{end_time - start_time} s")
        return result

    return wrapper


@log_runtime
def main():
    """
        Main function that orchestrates the bridge image downloading process.
        """
    base_url_suffix = '/de'
    base_url = base_URL + base_url_suffix
    if language == "English":
        key_mapping = {"Structure": "Structure type", "Material": "Bridge type"}
    else:
        key_mapping = {"Baustoff": "Brücke typ"}

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

    create_folder(image_folder)
    create_folder(output_folder)
    if not os.listdir(output_folder):
        copy_all_templates()

    login_choice = input("Would you like to log in? (y/n): ").lower()

    if login_choice == 'y':
        driver.set_window_size(window_size_width, window_size_height)
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
            download_images_by_bridge_name(driver, bridge_names, base_url, key_mapping)
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
                download_images_by_bridge_type(driver, bridge_type, num_bridges, base_url, key_mapping, country_code)
            else:
                download_images_by_bridge_type(driver, bridge_type, num_bridges, base_url, key_mapping)
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
    """
        Entry point of the script. Repeatedly executes the main function based on user input.
        """
    again = "y"
    while again == "y":
        main()
        again = input("Do you want to do it again?(y/n)")
