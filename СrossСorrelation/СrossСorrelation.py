from collections import defaultdict
from typing import List, Dict, Tuple
import pyhdfs
from faker import Faker
import random


### Cross Correlation Pairs
# Mapper
def map_pairs(order):
    items = order['items']
    pairs = []
    for i in range(len(items)):
        for j in range(i+1, len(items)):
            pairs.append(((items[i], items[j]), 1))
            pairs.append(((items[j], items[i]), 1))
    return pairs


# Reducer
def reduce_pairs(pairs):
    counts = defaultdict(int)
    for pair, count in pairs:
        counts[pair] += count
    return counts


# Пример использования
orders = [{'items': ['apple', 'banana']}, {'items': ['apple', 'cherry']}, {'items': ['banana', 'cherry']}]
mapped = [map_pairs(order) for order in orders]
pairs = [pair for sublist in mapped for pair in sublist]
result = reduce_pairs(pairs)
#print(result)

### Cross Correlation Stripes


# Mapper
def map_stripes(order):
    items = order['items']
    stripes = []
    for item in items:
        stripe = defaultdict(int)
        for other_item in items:
            if other_item != item:
                stripe[other_item] += 1
        stripes.append((item, stripe))
    return stripes


# Reducer
def reduce_stripes(stripes_list):
    counts = defaultdict(lambda: defaultdict(int))
    for item, stripe in stripes_list:
        for other_item, count in stripe.items():
            counts[item][other_item] += count
    return counts


# Пример использования
orders = [{'items': ['apple', 'banana']}, {'items': ['apple', 'cherry']}, {'items': ['banana', 'cherry']}]
mapped = [map_stripes(order) for order in orders]
stripes_list = [stripe for sublist in mapped for stripe in sublist]
result = reduce_stripes(stripes_list)
#print(result)

def generate_orders(num_orders: int, products: List[str], max_items: int = 5) -> List[Dict]:
    """
    Генерирует список заказов со случайным набором товаров.

    :param num_orders: Количество заказов.
    :param products: Список доступных товаров.
    :param max_items: Максимальное количество товаров в одном заказе.
    :return: Список заказов.
    """
    fake = Faker()
    orders = []

    for _ in range(num_orders):
        num_items = random.randint(1, min(max_items, len(products)))  # Учитываем количество доступных продуктов
        order_items = random.choices(products, k=num_items)  # Разрешаем повторение товаров
        order = {
            'order_id': fake.uuid4(),
            'items': order_items
        }
        orders.append(order)

    return orders

products = ["apple", "banana", "cherry", "date"]
orders = generate_orders(100, products)

#orders[:5]  # Показываем первые 5 заказов для проверки


# Список продуктов
products = ["apple", "banana", "cherry", "date"]

# Генерация заказов
orders = generate_orders(100, products)

# Применяем Map
mapped_orders = [map_pairs(order) for order in orders]

# Собираем все пары вместе перед Reduce
pairs_to_reduce = [pair for sublist in mapped_orders for pair in sublist]

# Применяем Reduce и получаем итоговые кросс-корреляции
cross_correlation_results = reduce_pairs(pairs_to_reduce)

# Выводим результаты
for pair, count in cross_correlation_results.items():
    print(f"Pair: {pair}, Count: {count}")




def read_cross_correlation_from_hdfs(path: str) -> dict:
    """
    Читает результаты кросс-корреляции из файла в HDFS.

    :param path: Путь к файлу в HDFS.
    :return: Словарь кросс-корреляции.
    """
    fs = pyhdfs.HdfsClient(hosts='localhost:50070')  # Подключение к HDFS
    with fs.open(path) as file:
        data = file.read().decode('utf-8')
        return eval(data)  # Преобразование строки в словарь

def recommend_products(product: str, cross_correlation: dict, top_n: int = 10) -> list:
    """
    Рекомендует топ-N товаров, наиболее часто покупаемых с заданным товаром.

    :param product: Название товара.
    :param cross_correlation: Словарь кросс-корреляции.
    :param top_n: Количество рекомендуемых товаров.
    :return: Список рекомендуемых товаров.
    """
    related_products = defaultdict(int)

    for (prod1, prod2), count in cross_correlation.items():
        if prod1 == product:
            related_products[prod2] += count
        elif prod2 == product:
            related_products[prod1] += count

    # Сортировка товаров по убыванию корреляции и выборка топ-N
    recommended = sorted(related_products.items(), key=lambda x: x[1], reverse=True)[:top_n]

    return [prod for prod, _ in recommended]


# Путь к файлу в HDFS
hdfs_path = "/path/to/your/cross_correlation_file"

# Чтение кросс-корреляции из HDFS
cross_correlation_data = read_cross_correlation_from_hdfs(hdfs_path)

# Получение рекомендаций
product_to_recommend = "apple"
recommendations = recommend_products(product_to_recommend, cross_correlation_data)

# Вывод рекомендаций
print(f"Рекомендации для товара {product_to_recommend}: {recommendations}")

from collections import defaultdict

def read_cross_correlation_from_hdfs(path: str) -> dict:
    """
    Читает результаты кросс-корреляции из файла в HDFS.

    :param path: Путь к файлу в HDFS.
    :return: Словарь кросс-корреляции.
    """
    fs = pyhdfs.HdfsClient(hosts='localhost:50070')  # Подключение к HDFS
    with fs.open(path) as file:
        data = file.read().decode('utf-8')
        return eval(data)  # Преобразование строки в словарь

def recommend_products(product: str, cross_correlation: dict, top_n: int = 10) -> list:
    """
    Рекомендует топ-N товаров, наиболее часто покупаемых с заданным товаром.

    :param product: Название товара.
    :param cross_correlation: Словарь кросс-корреляции.
    :param top_n: Количество рекомендуемых товаров.
    :return: Список рекомендуемых товаров.
    """
    related_products = defaultdict(int)

    for (prod1, prod2), count in cross_correlation.items():
        if prod1 == product:
            related_products[prod2] += count
        elif prod2 == product:
            related_products[prod1] += count

    # Сортировка товаров по убыванию корреляции и выборка топ-N
    recommended = sorted(related_products.items(), key=lambda x: x[1], reverse=True)[:top_n]

    return [prod for prod, _ in recommended]


# Путь к файлу в HDFS
hdfs_path = "/path/to/your/cross_correlation_file"

# Чтение кросс-корреляции из HDFS
cross_correlation_data = read_cross_correlation_from_hdfs(hdfs_path)

# Получение рекомендаций
product_to_recommend = "apple"
recommendations = recommend_products(product_to_recommend, cross_correlation_data)

# Вывод рекомендаций
print(f"Рекомендации для товара {product_to_recommend}: {recommendations}")
