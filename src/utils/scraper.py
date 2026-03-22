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

    # print("\nTop level keys:")
    # print(data.keys())

    # print("\nKeys in 'search':")
    # print(data["search"].keys())

    # print("\nKeys in 'config':")
    # print(data["config"].keys())
    # #print(data["search"])

    # print("\nSample property keys:")
    # print(data["search"]['list'][0].keys())

    # print("\nKeys in 'listings':")
    # print(data["search"]['listings'][0].keys())

    # print("\nExtracting property characteristic...")
    # print(data["search"]['listings'][0]['characteristic'].keys())

    # print("\nExtracting property address...")
    # print(data["search"]['listings'][0]['address'].keys())

    # print("=" * 40)
    
    property_list = data["search"]["listings"]

    properties_output = []
    # list of fields explicitly printed below; output will include only these
    general_keys = [
        "id",
        "type",
        "permalink",
        "isNewBuild",
        "createdAt",
        "updatedAt",
        "price",
        "soldPrice",
        "baselinePrice",
        "previewDescriptions",
        "address",
        "characteristic"
    ]
    address_keys = ["street", "postalCode", "city", "country"] #TODO
    characteristic_keys = ['rooms', 'bedrooms', 'bathrooms', 'showers', 'basement', 'garages', 'indoorParking', 'outdoorParking', 'surface', 'groundSurface'] #TODO
    
    #dict_keys(['id', 'externalReference', 'type', 'typeKey', 'typeId', 'group', 'groupId', 'format', 'permalink', 'isNewBuild', 'createdAt', 'updatedAt', 'soldPrice', 'baselinePrice', 'status', 'transaction', 'publishTo', 'address', 'contact', 'media', 'errors', 'previewDescription', 'previewDescriptions', 'price', 'isPriceOnDemand', 'characteristic'])

    for property in property_list:
        # collect values as text-only dictionary for just the printed keys
        property_info = {}
        for key in general_keys:
            if key in ["address", "characteristic"]:
                # for nested dicts, extract only the specified keys
                if key == "address":
                    nested_dict = property.get(key, {})
                    for nested_key in address_keys:
                        property_info[f"{nested_key}"] = str(nested_dict.get(nested_key, ""))
                elif key == "characteristic":
                    nested_dict = property.get(key, {})
                    for nested_key in characteristic_keys:
                        property_info[f"{nested_key}"] = str(nested_dict.get(nested_key, ""))
            else:
                property_info[key] = str(property.get(key, ""))
        properties_output.append(property_info)

    if is_save_json:
        # after loop, write to JSON file
        output_path = "data/properties.json"
        with open(output_path, "w", encoding="utf-8") as out_f:
            json.dump(properties_output, out_f, ensure_ascii=False, indent=2)
        print(f"wrote {len(properties_output)} entries to {output_path}")
        
    return properties_output

if __name__ == "__main__":
    property_scraper(url, is_save_json=True)