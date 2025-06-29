import time

import requests
def complex_queries(bucket_name="yelp"):
    """Имплементација на сложени прашалници"""

    # Превземи ги сите клучеви еднаш
    keys_response = requests.get(f"http://localhost:8098/buckets/{bucket_name}/keys?keys=true")
    keys = keys_response.json().get("keys", [])
    print(f"Вкупно клучеви во bucket '{bucket_name}': {len(keys)}")

    # 1. Пронајди ги сите ресторани во Санта Барбара со повеќе од 100 рецензии
    def restaurants_in_la_with_reviews():
        print("Барање: Ресторани во Санта Барбара со 50+ рецензии")
        start = time.time()

        # Мора да се превземат сите клучеви
        keys_response = requests.get(f"http://localhost:8098/buckets/{bucket_name}/keys?keys=true")
        keys = keys_response.json().get("keys", [])

        matching_businesses = []
        for key in keys:
            response = requests.get(f"http://localhost:8098/buckets/{bucket_name}/keys/{key}")
            if response.status_code == 200:
                data = response.json()
                if (data.get("city", "").lower() == "santa barbara" and
                        data.get("review_count", 0) > 100 and
                        "restaurant" in data.get("categories", "").lower()):
                    matching_businesses.append(data)

        end = time.time()
        print(f"Пронајдени {len(matching_businesses)} ресторани во {end - start:.2f} сек.")
        return matching_businesses

    # 2. Пресметај просечна оценка по градови
    def average_rating_by_city():
        print("Барање: Просечна оценка по градови")
        start = time.time()

        keys_response = requests.get(f"http://localhost:8098/buckets/{bucket_name}/keys?keys=true")
        keys = keys_response.json().get("keys", [])

        city_ratings = {}
        for key in keys[:1000]:
            response = requests.get(f"http://localhost:8098/buckets/{bucket_name}/keys/{key}")
            if response.status_code == 200:
                data = response.json()
                city = data.get("city", "Unknown")
                rating = data.get("stars", 0)

                if city not in city_ratings:
                    city_ratings[city] = []
                city_ratings[city].append(rating)

        # Пресметај просеци
        city_averages = {}
        for city, ratings in city_ratings.items():
            city_averages[city] = sum(ratings) / len(ratings)

        end = time.time()
        print(f"Анализирани {len(city_averages)} градови во {end - start:.2f} сек.")
        return city_averages

    # 3. Топ 10 најпопуларни категории
    def top_categories():
        print("Барање: Топ 10 најпопуларни категории")
        start = time.time()

        keys_response = requests.get(f"http://localhost:8098/buckets/{bucket_name}/keys?keys=true")
        keys = keys_response.json().get("keys", [])

        category_count = {}
        for key in keys[:2000]:  # Ограничи за тестирање
            response = requests.get(f"http://localhost:8098/buckets/{bucket_name}/keys/{key}")
            if response.status_code == 200:
                data = response.json()
                categories = data.get("categories", "")
                if categories:
                    for category in categories.split(", "):
                        category = category.strip()
                        category_count[category] = category_count.get(category, 0) + 1

        # Сортирај по популарност
        top_categories = sorted(category_count.items(), key=lambda x: x[1], reverse=True)[:10]

        end = time.time()
        print(f"Анализирани категории во {end - start:.2f} сек.")
        return top_categories

    return {
        'la_restaurants': restaurants_in_la_with_reviews(),
        'city_averages': average_rating_by_city(),
        'top_categories': top_categories()
    }
if __name__ == "__main__":
    results = complex_queries()
    print("\n=== Резултати ===")
    print("Ресторани во ЛА со 100+ рецензии:", len(results['la_restaurants']))
    print("Просечна оценка по градови:", results['city_averages'])
    print("Топ 10 категории:", results['top_categories'])