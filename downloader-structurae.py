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


def get_bridge_soup(url):
    driver = webdriver.Chrome(
        service=Service(executable_path="C:\\Users\\Quantum\\Downloads\\Compressed\\chromedriver.exe"))
    driver.get(url)

    try:
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, "//div[@class='owl-item active center']"))
        )
    except TimeoutException:
        print("Timeout: the images could not be loaded within 30 seconds.")
        driver.quit()
        return None

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()
    return soup


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


def get_image_data(soup):
    image_containers = soup.find_all('div',
                                     {'class': ['owl-item cloned', 'owl-item active center', 'owl-item cloned active']})

    image_data = []
    for container in image_containers:
        img_tag = container.find('img', {'itemprop': 'photo'})
        if img_tag:
            src = img_tag.get('src')
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
    base_url = "https://structurae.net"
    problematic_bridges = []
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

        page_soup = get_bridge_soup(bridge_url)
        if page_soup is None:
            print(f"Failed to load the page for bridge: {bridge_name_to_download}")
            problematic_bridges.append(bridge_name_to_download)
            continue

        image_data = get_image_data(page_soup)

        bridge_info = get_bridge_info(page_soup)
        if bridge_info["Bridge Name"] is None:
            bridge_name = bridge_info.get('Bezeichnung', 'Unknown_bridge')
        else:
            bridge_name = bridge_info["Bridge Name"]
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
    print(f"Problematic bridges : {problematic_bridges}")
    input("Press the Enter key to exit the program...")


if __name__ == "__main__":
    main()
