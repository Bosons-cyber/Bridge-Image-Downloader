import os
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
    bridge_types = {
        "Balkenbrücken": "balkenbruecken",
        "Bewegliche Brücken": "bewegliche-bruecken",
        "Bogenbrücken": "bogenbruecken",
    }
    print("Please choose a bridge type:")
    for idx, (name, _) in enumerate(bridge_types.items(), 1):
        print(f"{idx}. {name}")
    choice = int(input("Enter the number of your choice: "))
    chosen_type = list(bridge_types.values())[choice - 1]
    return chosen_type


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


def download_image(url, save_path):
    if os.path.exists(save_path):
        print(f"File already exists, skip download: {save_path}")
        return

    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response, open(save_path, 'wb') as out_file:
            data = response.read()
            out_file.write(data)
    except urllib.error.URLError as e:
        print(f"Failed to download image: {url} -> {save_path}, reason: {e}")


def download_images_by_bridge_type(driver, bridge_type, num_bridges, country_code=None):
    base_url = "https://structurae.net"
    if country_code:
        bridge_type_url = f"{base_url}/de/bauwerke/bruecken/{bridge_type}/liste?filtercountry={country_code}"
    else:
        bridge_type_url = f"{base_url}/de/bauwerke/bruecken/{bridge_type}/liste"
    driver.get(bridge_type_url)

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "td > a.listableleft"))
        )
    except TimeoutException:
        print("Timeout: Unable to locate the bridge links.")
        return

    bridge_links = driver.find_elements(By.CSS_SELECTOR, "td > a.listableleft")
    bridge_urls = [link.get_attribute("href") for link in bridge_links[:num_bridges]]

    for idx, bridge_url in enumerate(bridge_urls, 1):
        print(f"Processing bridge {idx} of {num_bridges}...")

        bridge_media_soup = get_bridge_media_soup(driver, bridge_url)
        image_data = get_image_data(bridge_media_soup)

        bridge_info_soup = get_bridge_info_soup(driver, bridge_url)
        bridge_info = get_bridge_info(bridge_info_soup)

        bridge_name = bridge_info.get("Bridge Name", "Unknown_bridge")
        bridge_folder = os.path.join("images", bridge_name)
        if not os.path.exists(bridge_folder):
            os.makedirs(bridge_folder)

        save_bridge_info(bridge_info, os.path.join(bridge_folder, 'bridge_info.txt'))

        high_res_image_urls = [data["murl"] for data in image_data]
        for idx, high_res_image_url in enumerate(high_res_image_urls):
            save_path = os.path.join(bridge_folder, f"image_{idx}.jpg")
            download_image(high_res_image_url, save_path)
            print(f"Downloaded image: {save_path}")

        time.sleep(1)

    print("All bridges processed!")


def get_image_data(soup):
    image_entries = soup.find_all('div', class_='jg-entry')

    image_data = []
    for entry in image_entries:
        link = entry.find('a', class_='imageThumbLink_2')
        if link:
            img = link.find('img')
            if img:
                src = img.get('src')
                if src and src.startswith("http"):
                    image_data.append({"murl": src})

    return image_data


def format_bridge_name_for_url(bridge_name):
    return bridge_name.lower().replace(" ", "-")


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


def replace_german_chars(text):
    replacements = {
        'ä': 'ae',
        'ö': 'oe',
        'ü': 'ue',
        'ß': 'ss',
        'Ä': 'Ae',
        'Ö': 'Oe',
        'Ü': 'Ue'
    }

    for german_char, replacement in replacements.items():
        text = text.replace(german_char, replacement)

    return text


def save_bridge_info(bridge_info, file_path):
    with open(file_path, 'w', encoding='utf-8') as f:
        for key, value in bridge_info.items():
            f.write(f"{key}: {value}\n")


def main():
    base_url = "https://structurae.net/de/"
    problematic_bridges = []
    login_choice = input("Would you like to log in? (y/n): ").lower()
    driver = webdriver.Chrome(
        service=Service(executable_path="C:\\Users\\Quantum\\Bridge-Image-Downloader\\chromedriver-win64"
                                        "\\chromedriver.exe"))

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
        except FileNotFoundError:
            print("Please create a bridge.txt and add the name of the bridge to be processed line by line in it.")
            input("Press the Enter key to exit the program...")

        for bridge_name_to_download in bridge_names:
            print(f"Processing bridge: {bridge_name_to_download}")
            bridge_name_to_download = replace_german_chars(bridge_name_to_download)
            formatted_bridge_name = format_bridge_name_for_url(bridge_name_to_download)

            bridge_url = f"{base_url}/de/bauwerke/{formatted_bridge_name}"

            response = requests.get(bridge_url)

            if response.status_code != 200:
                print(f"Bridge not found: {bridge_name_to_download}")
                problematic_bridges.append(bridge_name_to_download)
                continue

            bridge_info_soup = get_bridge_info_soup(driver, bridge_url)
            bridge_info = get_bridge_info(bridge_info_soup)
            if bridge_info["Bridge Name"] is None:
                bridge_name = bridge_info.get('Bezeichnung', 'Unknown_bridge')
            else:
                bridge_name = bridge_info["Bridge Name"]
            bridge_folder = os.path.join("images", bridge_name)
            if not os.path.exists(bridge_folder):
                os.makedirs(bridge_folder)

            save_bridge_info(bridge_info, os.path.join(bridge_folder, 'bridge_info.txt'))

            bridge_media_soup = get_bridge_media_soup(driver, bridge_url)
            image_data = get_image_data(bridge_media_soup)
            if not image_data:
                print(f"Can't find the image: {bridge_name_to_download}")
                problematic_bridges.append(bridge_name_to_download)
            else:
                high_res_image_urls = [data["murl"] for data in image_data]
                for idx, high_res_image_url in enumerate(high_res_image_urls):
                    save_path = os.path.join(bridge_folder, f"image_{idx}.jpg")
                    download_image(high_res_image_url, save_path)
                    print(f"Downloaded image: {save_path}")

            time.sleep(1)
        print(f"Problematic bridges : {problematic_bridges}")
    elif chosen_type == 'type':
        bridge_type = choose_bridge_type()
        num_bridges = int(input("How many bridges do you want to download? "))
        country_mode = input("Do you want to search by country?(login needed)(y/n): ").lower()
        if country_mode:
            country_code = input(
                "Please enter the country code (e.g., DE for Germany, BE for Belgium): ").strip().upper()
            download_images_by_bridge_type(driver, bridge_type, num_bridges, country_code)
        else:
            download_images_by_bridge_type(driver, bridge_type, num_bridges, login_choice)
    else:
        print("Invalid mode selected. Exiting.")
        time.sleep(5)
        return

    input("Press the Enter key to exit the program...")
    driver.quit()


if __name__ == "__main__":
    main()
