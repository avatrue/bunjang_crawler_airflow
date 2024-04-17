import json
from cassandra.cluster import Cluster

# Cassandra 클러스터에 연결
cluster = Cluster(['localhost'])
session = cluster.connect('bunjang')

# JSON
with open('../output/all_products.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Cassandra에 데이터 삽입
for product in data:
    session.execute(
        """
        INSERT INTO products (pid, brands, name, price_updates, product_image, status, category_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            product['pid'],
            product['brands'],
            product['name'],
            product['price_updates'],
            product['product_image'],
            product['status'],
            product['category_id']
        )
    )

print("데이터 저장완료.")
