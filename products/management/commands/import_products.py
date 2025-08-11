from django.core.management.base import BaseCommand
from products.models import Product, Attribute, ProductAttributeValue, ProductCategory
from decimal import Decimal
import csv
import os

class Command(BaseCommand):
    help = 'Импорт товаров из CSV'

    def add_arguments(self, parser):
        parser.add_argument('csv_file', type=str, help='Путь к CSV файлу')

    def handle(self, *args, **options):
        csv_file_path = options['csv_file']
        if not os.path.exists(csv_file_path):
            self.stderr.write(f"Файл не найден: {csv_file_path}")
            return

        with open(csv_file_path, 'r', encoding='utf-8-sig') as csvfile:
            reader = list(csv.reader(csvfile, delimiter=';'))

        product_names = reader[0][1:]  # названия товаров (B-L)
        categories = reader[1][1:]     # названия категорий (B-L)

        print('Названия товаров:', product_names)
        print('Категории:', categories)

        for col_index, (product_name, category_name) in enumerate(zip(product_names, categories), start=1):
            if not product_name or not product_name.strip():
                continue
            if not category_name or not category_name.strip():
                print(f"Пропуск товара {product_name}: не указана категория")
                continue

            # Получаем/создаём категорию
            category, _ = ProductCategory.objects.get_or_create(
                title=category_name.strip()
            )

            product, created = Product.objects.get_or_create(
                title=product_name.strip(),
                defaults={
                    'price': Decimal('0.00'),
                    'category': category,
                }
            )
            self.stdout.write(f"{'Создан' if created else 'Обновлён'} товар: {product.title} (категория: {category.title})")

            # Перебор характеристик для этого товара
            for row in reader[2:]:
                attr_name = row[0].strip().lower()
                if col_index >= len(row):
                    continue
                attr_value = row[col_index].strip()
                if not attr_value or attr_value == '-':
                    continue

                # # Определяем value_type
                # value_clean = attr_value.strip().lower()
                # if value_clean in ['да', 'нет']:
                #     value_type = 'bool'
                # else:
                #     value_type = 'str'

                # Создаём характеристику при необходимости
                attribute, attr_created = Attribute.objects.get_or_create(
                    name=attr_name,
                    defaults={'value_type': 'str'}
                )
                if attr_created:
                    self.stdout.write(f"Создан атрибут: {attribute.name}")

                pav, pav_created = ProductAttributeValue.objects.update_or_create(
                    product=product,
                    attribute=attribute,
                    defaults={'value': attr_value}
                )
                action = 'создано' if pav_created else 'обновлено'
                self.stdout.write(f"Значение {action}: {attribute.name} = {attr_value}")

        self.stdout.write(self.style.SUCCESS('Импорт завершён!'))
