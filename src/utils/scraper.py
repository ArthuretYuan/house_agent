import requests
from bs4 import BeautifulSoup
import json
import re




url = "https://www.athome.lu/vente?page=6"

def property_scraper(url, is_save_json=False):
    """Scrape property data from the given URL and return a list of dictionaries
    with the relevant fields."""

    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    script_text = None

    # 找到包含 INITIAL_STATE 的 script
    for script in soup.find_all("script"):
        if script.string and "window.__INITIAL_STATE__" in script.string:
            script_text = script.string
            break

    if not script_text:
        print("INITIAL_STATE not found")
        exit()

    # 🔥 专用 regex
    pattern = r"window\.__INITIAL_STATE__\s*=\s*({.*?})\s*;"

    match = re.search(pattern, script_text, re.DOTALL)

    if not match:
        print("JSON not matched")
        exit()

    json_text = match.group(1)
    json_text = json_text.replace("undefined", "null")
    json_text = json_text.replace("NaN", "null")
    json_text = json_text.replace("Infinity", "null")

    data = json.loads(json_text)
    #json.dump(data, open("data.json", "w"), indent=4)

    print("Top level keys:")
    print(data.keys())
    print(data["search"].keys())
    print(data["config"].keys())
    #print(data["search"])
    print(data["search"]['list'][0].keys())
    print(data["search"]['listings'][0].keys())
    print("=" * 40)


    property_list = data["search"]["list"]

    properties_output = []
    # list of fields explicitly printed below; output will include only these
    printed_keys = [
        "id",
        "isSoldProperty",
        "propertyType",
        "propertySubType",
        "isNewBuild",
        "buildingYear",
        "mandate",
        "description",
        "price",
        "price_min",
        "price_max",
        "propertySurface",
        "minPropertySurface",
        "maxPropertySurface",
        "floorNumber",
        "roomsCount",
        "minRoomsCount",
        "maxRoomsCount",
        "energy",
        "geo",
    ]

    for property in property_list:
        print("Property details:")
        print(f"ID: {property['id']}\nSold: {property['isSoldProperty']}\nPropertyType: {property['propertyType']}\nPropertySubType: {property['propertySubType']}\nIsNewBuild: {property['isNewBuild']}\nBuildingYear: {property['buildingYear']}\nMandate: {property['mandate']}\nDescription: {property['description']}\nPrice: {property['price']}\nPriceMin: {property['price_min']}\nPriceMax: {property['price_max']}\nPropertySurface: {property['propertySurface']}\nMinPropertySurface: {property['minPropertySurface']}\nMaxPropertySurface: {property['maxPropertySurface']}\nFloorNumber: {property['floorNumber']}\nRoomsCount: {property['roomsCount']}\nMinRoomsCount: {property['minRoomsCount']}\nMaxRoomsCount: {property['maxRoomsCount']}\nEnergy: {property['energy']}\nGeo: {property['geo']}")
        print("-" * 40)

        # collect values as text-only dictionary for just the printed keys
        properties_output.append({k: str(property.get(k, "")) for k in printed_keys})

    if is_save_json:
        # after loop, write to JSON file
        output_path = "data/properties.json"
        with open(output_path, "w", encoding="utf-8") as out_f:
            json.dump(properties_output, out_f, ensure_ascii=False, indent=2)
        print(f"wrote {len(properties_output)} entries to {output_path}")

    return properties_output

if __name__ == "__main__":
    property_scraper(url, is_save_json=True)