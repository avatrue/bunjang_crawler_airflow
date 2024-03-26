import requests
from urllib.parse import quote
import json
import os

def get_total_count(brands, category_id):
    base_url = "https://api.bunjang.co.kr/api/1/find_v2.json"
    params = {
        "q": brands[0],
        "order": "score",
        "page": 0,
        "f_category_id": category_id,
        "n": 100,
        "stat_category_required": 1
    }

    response = requests.get(base_url, params=params)
    print(f"전체 제품 수 조회 API: {response.url}")
    data = response.json()

    total_count = data["categories"][0]["count"]
    return total_count

def get_product_data(brands, category_id, page):
    base_url = "https://api.bunjang.co.kr/api/1/find_v2.json"
    params = {
        "q": brands[0],
        "order": "score",
        "page": page,
        "f_category_id": category_id,
        "n": 100,
        "stat_category_required": 1
    }

    response = requests.get(base_url, params=params)
    print(f"제품 데이터 조회 API (페이지 {page + 1}): {response.url}")
    data = response.json()

    product_list = []
    for product in data["list"]:
        price = product["price"]
        product_info = {
            "pid": product["pid"],
            "brands": [brand for brand in brands if brand in product["name"]],
            "name": product["name"],
            "price_updates": [{product["update_time"]: price}],
            "product_image": product["product_image"],
            "status": product["status"],
            "category_id": product["category_id"]
        }
        product_list.append(product_info)

    return data["no_result"], data["categories"][0]["count"], data["list"], product_list

def update_products(all_products, new_products):
    for new_product in new_products:
        for product in all_products:
            if product["pid"] == new_product["pid"]:
                for brand in new_product["brands"]:
                    if brand not in product["brands"]:
                        product["brands"].append(brand)
                for update in new_product["price_updates"]:
                    update_time = list(update.keys())[0]
                    if update_time not in [list(p.keys())[0] for p in product["price_updates"]]:
                        product["price_updates"].insert(0, update)
                if new_product["status"] != product["status"]:
                    product["status"] = new_product["status"]
                break
        else:
            all_products.append(new_product)

    return all_products

def save_to_json(data, filename):
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

def extract_categories(categories, threshold=30000, include_parent=False):
    result = []
    for category in categories:
        if category["count"] > threshold:
            if include_parent:
                result.append({"id": category["id"], "count": category["count"]})
            if "categories" in category:
                result.extend(extract_categories(category["categories"], threshold, False))
        else:
            result.append({"id": category["id"], "count": category["count"]})
    return result

def collect_and_filter_data(brands, output_file):
    filtered_products = []

    base_url = "https://api.bunjang.co.kr/api/1/find_v2.json"
    params = {
        "q": brands[0],
        "order": "score",
        "page": 0,
        "f_category_id": 320,
        "n": 100,
        "stat_category_required": 1
    }

    response = requests.get(base_url, params=params)
    data = response.json()

    top_level_categories = data["categories"]
    filtered_categories = [{"id": top_level_categories[0]["id"], "count": top_level_categories[0]["count"]}]
    filtered_categories.extend(extract_categories(top_level_categories, include_parent=False))

    total_count = filtered_categories[0]["count"]
    print(f"브랜드 {brands[0]} - 전체 제품 수: {total_count}")

    for category in filtered_categories[1:]:
        category_id = category["id"]
        page = 0
        while True:
            print(f"{page + 1} 페이지 데이터 수집 중...")
            no_result, total_count, products, collected_products = get_product_data(brands, category_id, page)
            filtered_products.extend(filter_products(collected_products))

            if no_result:
                break

            page += 1
            if page == 300:
                break

    save_to_json(filtered_products, output_file)
    print(f"브랜드 {brands[0]} - 필터링 후 남은 제품 수: {len(filtered_products)}")
    print()

def filter_products(products):
    filtered_products = []
    for product in products:
        price_updates = product["price_updates"]
        latest_price = list(price_updates[0].values())[0]
        if not any(brand in product["name"] for brand in product["brands"]) or not latest_price.isdigit() or latest_price[-1] != "0" or int(latest_price) < 10000:
            continue
        filtered_products.append(product)
    return filtered_products

def merge_results(input_dir, output_file):
    all_products = []
    for filename in os.listdir(input_dir):
        if filename.endswith(".json"):
            with open(os.path.join(input_dir, filename), "r", encoding="utf-8") as file:
                products = json.load(file)
                all_products = update_products(all_products, products)
    save_to_json(all_products, output_file)
    print("모든 브랜드 데이터 병합 및 업데이트 완료")