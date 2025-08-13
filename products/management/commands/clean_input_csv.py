# products/management/commands/clean_input_csv.py
import csv
from collections import defaultdict

from django.core.management.base import BaseCommand, CommandError

class Command(BaseCommand):
    help = "Делает 'чистовик': оставляет по одной записи для каждой пары (код, название). "\
           "Пишет два файла: unique.csv (очищенный) и removed_exact_duplicates.csv (удалённые дубли)."

    def add_arguments(self, p):
        p.add_argument('--file', required=True, help='Путь к исходному CSV (UTF-8). Должны быть колонки: код, название.')
        p.add_argument('--out-dir', default='.', help='Куда сохранять файлы (по умолчанию текущая папка)')

    def handle(self, *args, **opts):
        path = opts['file']
        out_dir = opts['out_dir'].rstrip('/')

        # читаем с автоопределением разделителя и нормализуем заголовки
        def _norm_key(k: str) -> str:
            return (k or '').replace('\ufeff', '').strip().lower()

        try:
            with open(path, 'r', encoding='utf-8', newline='') as f:
                sample = f.read(4096); f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=[',',';','\t'])
                except csv.Error:
                    dialect = csv.get_dialect('excel'); dialect.delimiter = ','
                reader = csv.reader(f, dialect)
                rows = list(reader)
        except FileNotFoundError:
            raise CommandError(f'Файл не найден: {path}')
        if not rows:
            raise CommandError('Пустой CSV')

        header = { _norm_key(h): idx for idx, h in enumerate(rows[0]) }
        columns = rows[0]  # сохраняем порядок исходных колонок

        def col(row, name, alt=None):
            idx = header.get(name) or (header.get(alt) if alt else None)
            return (row[idx].strip() if idx is not None and idx < len(row) else '')

        # проходим строки и оставляем только первую для каждой пары (код, название)
        seen = set()
        unique_rows = [columns]   # с заголовком
        removed = []

        for i, row in enumerate(rows[1:], start=2):
            code = col(row, 'код', 'code')
            name = col(row, 'название', 'name')
            key = (code, name)  # ровно как есть, только strip

            if not code and not name:
                # пустые строки пропускаем бесшумно
                continue

            if key in seen:
                removed.append({
                    'row': i,
                    'code': code,
                    'name': name,
                })
                continue

            seen.add(key)
            unique_rows.append(row)

        # пишем unique.csv тем же разделителем, что входной
        unique_path = f"{out_dir}/unique.csv"
        with open(unique_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, dialect)
            writer.writerows(unique_rows)

        # и список удалённых точных дублей
        removed_path = f"{out_dir}/removed_exact_duplicates.csv"
        with open(removed_path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['row','code','name'])
            w.writeheader()
            for r in removed:
                w.writerow(r)

        self.stdout.write(self.style.SUCCESS(
            f"Готово.\n"
            f" – unique.csv: {len(unique_rows)-1} строк (без точных дублей)\n"
            f" – removed_exact_duplicates.csv: {len(removed)} удалённых дублей\n"
            f"Папка: {out_dir}"
        ))
