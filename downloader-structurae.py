import csv
import os
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import ssl
import requests

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

BASE_URL = "https://structurae.net"
USER_AGENT = 'Mozilla/5.0'
IMAGE_FOLDER = "images"


def navigate_and_wait(driver, url):
    driver.set_window_size(1200, 800)
    driver.get(url)
    return BeautifulSoup(driver.page_source, 'html.parser')


def get_full_bridge_url(country_code, bridge_type, base_usl):
    if country_code:
        final_address = f"{base_usl}/bauwerke/bruecken/{bridge_type}/liste?filtercountry={country_code}"
        return final_address
    else:
        final_address = f"{base_usl}/bauwerke/bruecken/{bridge_type}/liste"
        return final_address


def create_bridge_folder(bridge_name):
    clean_name = clean_folder_name(bridge_name)
    bridge_folder = os.path.join(IMAGE_FOLDER, clean_name)
    if not os.path.exists(bridge_folder):
        os.makedirs(bridge_folder)
    return bridge_folder


def get_bridge_info_soup(driver, url):
    driver.set_window_size(1200, 800)
    driver.get(url)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    return soup


def get_bridge_media_soup(driver, url):
    media_url = f"{url}/medien"
    driver.set_window_size(1200, 800)
    driver.get(media_url)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a.imageThumbLink_2"))
        )
    except TimeoutException:
        print("No image found for the bridge. Continuing to extract other information...")

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    return soup


def choose_bridge_type():

    text = input("Please enter the name of the bridge type: ")
    typename = format_text(text)

    return typename


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


def download_images_by_bridge_name(driver, bridge_names, base_url):
    problematic_bridges = []
    global BASE_URL

    summary_csv_path_en = "images/summary_en.csv"
    summary_csv_path_de = "images/summary_de.csv"

    for bridge_name_to_download in bridge_names:
        print(f"Processing bridge: {bridge_name_to_download}")
        formatted_bridge_name = format_text(bridge_name_to_download)

        bridge_url_de = f"{base_url}/bauwerke/{formatted_bridge_name}"

        response = requests.get(bridge_url_de)

        if response.status_code != 200:
            print(f"Bridge not found: {bridge_name_to_download}")
            problematic_bridges.append(bridge_name_to_download)
            continue

        bridge_info_soup_de = get_bridge_info_soup(driver, bridge_url_de)
        bridge_info_de = get_bridge_info(bridge_info_soup_de)
        if bridge_info_de["Bridge Name"] is None:
            bridge_name = bridge_info_de.get('Bezeichnung', 'Unknown_bridge')
        else:
            bridge_name = bridge_info_de["Bridge Name"]
        clean_name = clean_folder_name(bridge_name)
        bridge_folder = os.path.join("images", clean_name)
        if not os.path.exists(bridge_folder):
            os.makedirs(bridge_folder)

        append_bridge_info_to_csv(bridge_info_de, summary_csv_path_de)

        bridge_url_en = get_en_link(bridge_info_soup_de)
        bridge_info_soup_en = navigate_and_wait(driver, BASE_URL + bridge_url_en)
        bridge_info_en = get_bridge_info(bridge_info_soup_en)

        append_bridge_info_to_csv(bridge_info_en, summary_csv_path_en)

        bridge_media_soup = get_bridge_media_soup(driver, bridge_url_de)
        image_data = get_image_data(bridge_media_soup)
        if not image_data:
            print(f"Can't find the image: {bridge_name_to_download}")
            problematic_bridges.append(bridge_name_to_download)
        else:
            high_res_image_urls = image_data
            for idx, high_res_image_url in enumerate(high_res_image_urls):
                response = requests.get(BASE_URL + high_res_image_url)
                new_soup = BeautifulSoup(response.content, 'html.parser')

                download_link = get_download_link(new_soup)
                if download_link:
                    save_path = os.path.join(bridge_folder, f"image_{idx}.jpg")
                    download_image(download_link, save_path)
                    print(f"Downloaded image: {save_path}")
        time.sleep(1)

    print(f"Problematic bridges : {problematic_bridges}")


def download_images_by_bridge_type(driver, bridge_type, num_bridges, base_url, country_code=None):
    global BASE_URL

    summary_csv_path_en = "images/summary_en.csv"
    summary_csv_path_de = "images/summary_de.csv"

    bridge_type_url = get_full_bridge_url(country_code, bridge_type, base_url)

    all_bridge_urls = []
    page = 0

    while len(all_bridge_urls) < num_bridges:
        current_page_url = bridge_type_url if page == 0 else f"{bridge_type_url}?min={page * 100}"
        navigate_and_wait(driver, current_page_url)

        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "td > a.listableleft"))
            )
            bridge_links = driver.find_elements(By.CSS_SELECTOR, "td > a.listableleft")
        except TimeoutException:
            print("Timed out waiting for page to load")
            return

        if not bridge_links:
            break

        bridge_urls = [link.get_attribute("href") for link in bridge_links]
        all_bridge_urls.extend(bridge_urls)

        page += 1

    downloaded_count = 0
    for idx, bridge_url_de in enumerate(all_bridge_urls, 1):
        bridge_info_soup_de = navigate_and_wait(driver, bridge_url_de)
        bridge_info_de = get_bridge_info(bridge_info_soup_de)
        bridge_name = bridge_info_de.get("Bridge Name", "Unknown_bridge")

        if os.path.exists(f"images/{bridge_name}"):
            print(f"Folder for bridge {bridge_name} already exists. Skipping...")
            continue

        print(f"Processing bridge {downloaded_count + 1} of {num_bridges}...")

        bridge_folder = create_bridge_folder(bridge_name)
        append_bridge_info_to_csv(bridge_info_de, summary_csv_path_de)

        bridge_url_en = get_en_link(bridge_info_soup_de)
        bridge_info_soup_en = navigate_and_wait(driver, BASE_URL + bridge_url_en)
        bridge_info_en = get_bridge_info(bridge_info_soup_en)

        append_bridge_info_to_csv(bridge_info_en, summary_csv_path_en)

        bridge_media_soup = get_bridge_media_soup(driver, bridge_url_de)
        image_data = get_image_data(bridge_media_soup)

        high_res_image_urls = image_data
        for idx, high_res_image_url in enumerate(high_res_image_urls):
            response = requests.get(BASE_URL + high_res_image_url)
            new_soup = BeautifulSoup(response.content, 'html.parser')

            download_link = get_download_link(new_soup)
            if download_link:
                save_path = os.path.join(bridge_folder, f"image_{idx}.jpg")
                download_image(download_link, save_path)
                print(f"Downloaded image: {save_path}")
        time.sleep(1)

        downloaded_count += 1
        if downloaded_count >= num_bridges:
            break

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


def download_image(url, save_path):
    if os.path.exists(save_path):
        print(f"File already exists, skip download: {save_path}")
        return

    try:
        req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(req) as response, open(save_path, 'wb') as out_file:
            data = response.read()
            out_file.write(data)
    except urllib.error.URLError as e:
        print(f"Failed to download image: {url} -> {save_path}, reason: {e}")


def format_text(text):
    name = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss").replace("Ä", "AE").replace("Ö", "Oe").replace("Ü", "Ue")
    name = name.replace(" ", "-").lower()
    return name


def get_bridge_info(soup):
    bridge_info = {}

    general_info_table = soup.find('div', {'class': 'js-acordion-body', 'id': 'general'}).find('table', {
        'class': 'aligned-tables'})
    typology_info_table = soup.find('div', {'class': 'js-acordion-body', 'id': 'typology'}).find('table', {
        'class': 'aligned-tables'})
    geographic_info_table = soup.find('div', {'class': 'js-acordion-body', 'id': 'geographic'}).find('table', {
        'class': 'aligned-tables'})

    for info_table in [general_info_table, typology_info_table, geographic_info_table]:
        if info_table:
            rows = info_table.find_all('tr')
            for row in rows:
                header = row.find('th')
                data = row.find('td')
                if header and data:
                    bridge_info[header.text.strip()] = data.text.strip()

    bridge_name_tag = soup.find("h1", {"itemprop": "name"})
    if bridge_name_tag:
        bridge_name = bridge_name_tag.get_text(strip=True)
        bridge_info["Bridge Name"] = bridge_name
    else:
        bridge_info["Bridge Name"] = "Unknown_bridge"

    return bridge_info


def get_existing_columns(file_path):
    if not os.path.exists(file_path):
        return []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        headers = next(reader, None)
        if headers:
            return headers
    return []


def append_bridge_info_to_csv(bridge_info, file_path):
    folder_path = os.path.dirname(file_path)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(list(bridge_info.keys()))

    existing_columns = get_existing_columns(file_path)
    all_columns = set(existing_columns) | set(bridge_info.keys())

    if len(all_columns) > len(existing_columns):
        temp_data = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for row in csv.reader(f):
                while len(row) < len(all_columns):
                    row.append("N/A")
                temp_data.append(row)

        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(list(existing_columns) + list(all_columns - set(existing_columns)))
            writer.writerows(temp_data)

    bridge_data = [bridge_info.get(column, "N/A") for column in existing_columns]

    with open(file_path, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(bridge_data)


def main():
    base_url_suffix = '/de'
    global BASE_URL
    base_url = BASE_URL + base_url_suffix

    try:
        driver = webdriver.Chrome(
            service=Service(executable_path="chromedriver.exe"))
    except Exception as e:
        print(e)
        return

    login_choice = input("Would you like to log in? (y/n): ").lower()

    if login_choice == 'y':
        driver.set_window_size(1200, 800)
        driver.get(base_url)
        try:
            login_button = driver.find_element(By.ID, "myStructuraeLoginBtn")
            if login_button:
                input("Please log in manually in your browser and press Enter to continue...")
        except:
            print("The login button was not found, you may have already logged in or the page structure has changed.")

    chosen_type = choose_search_type()
    if chosen_type == 'name':
        try:
            with open("bridges.txt", "r", encoding="utf-8") as file:
                bridge_names = [line.strip() for line in file]
            download_images_by_bridge_name(driver, bridge_names, base_url)
        except FileNotFoundError:
            print("Please create a bridge.txt and add the name of the bridge to be processed line by line in it.")
            input("Press the Enter key to exit the program...")
    elif chosen_type == 'type':
        bridge_type = choose_bridge_type()
        num_bridges = int(input("How many bridges do you want to download? "))
        country_mode = input("Do you want to search by country?(login needed)(y/n): ").lower()
        if country_mode == "y":
            country_code = input(
                "Please enter the country code (e.g., DE for Germany, BE for Belgium): ").strip().upper()
            download_images_by_bridge_type(driver, bridge_type, num_bridges, base_url, country_code)
        else:
            download_images_by_bridge_type(driver, bridge_type, num_bridges, base_url)
    else:
        print("Invalid mode selected. Exiting.")
        time.sleep(5)
        return

    driver.quit()


if __name__ == "__main__":
    main()
    input("Press the Enter key to exit the program...")