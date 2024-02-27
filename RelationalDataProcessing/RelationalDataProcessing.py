from collections import defaultdict

def mapper():
    for line in open("orders.csv"):
        _, _, productID, quantity = line.strip().split(',')
        print(f"{productID}\t{quantity}")


def reducer():
    current_product = None
    current_count = 0
    product_count = defaultdict(int)

    for line in input():
        product, count = line.strip().split('\t')

        if current_product == product:
            current_count += int(count)
        else:
            if current_product:
                product_count[current_product] += current_count
            current_count = int(count)
            current_product = product

    # Для последнего продукта
    if current_product == product:
        product_count[product] += current_count

    for product, count in product_count.items():
        print(f"{product}\t{count}")


