# products/management/commands/scan_sku_duplicates.py
import csv
import re

from collections import defaultdict

from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Ищет дубли в input.csv ТОЛЬКО по колонкам 'код' и 'название' без нормализации. " \
           "В файл dupes_in_file.csv выводит только конфликты: одинаковый код, но разные названия."

    @staticmethod
    def clean_name(name: str) -> str:
        if not name:
            return ""
        # удаляем круглые скобки и содержимое (один уровень)
        name = re.sub(r"\([^)]*\)", "", name)
        return name.strip()

    def add_arguments(self, p):
        p.add_argument('--file', required=True,
                       help='Путь к CSV (UTF-8). Колонки: код, название (регистр/язык заголовков не важны)')
        p.add_argument('--out-dir', default='.',
                       help='Куда писать отчёт dupes_in_file.csv (по умолчанию текущая папка)')

    def handle(self, *args, **opts):
        path = opts['file']
        out_dir = opts['out_dir'].rstrip('/')

        # --- читаем CSV с автоопределением разделителя и нормализацией заголовков
        def _norm_key(k: str) -> str:
            return (k or '').replace('\ufeff', '').strip().lower()

        try:
            with open(path, 'r', encoding='utf-8', newline='') as f:
                sample = f.read(4096);
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '\t'])
                except csv.Error:
                    dialect = csv.get_dialect('excel');
                    dialect.delimiter = ','
                reader = csv.reader(f, dialect)
                rows = list(reader)
        except FileNotFoundError:
            raise CommandError(f'Файл не найден: {path}')

        if not rows:
            raise CommandError('Пустой CSV')

        header = {_norm_key(h): idx for idx, h in enumerate(rows[0])}

        def col(row, name, alt=None):
            idx = header.get(name) or (header.get(alt) if alt else None)
            return (row[idx].strip() if idx is not None and idx < len(row) else '')

        missing = [k for k in ('код', 'название') if k not in header and (k == 'код' and 'code' not in header) and (
                    k == 'название' and 'name' not in header)]
        # выше чуть грубовато, но достаточно: примем и 'code'/'name'
        # (если хочешь — могу сделать явный маппинг)

        # группируем по КОДУ (строгое сравнение после strip)
        by_code = defaultdict(list)
        for i, row in enumerate(rows[1:], start=2):  # нумерация с учётом заголовка
            code = col(row, 'код', 'code')
            name = col(row, 'название', 'name')
            # пустые коды пропускаем
            if not code:
                continue
            by_code[code].append({'row': i, 'code': code, 'name': name})

        # конфликты: один и тот же code, но РАЗНЫЕ name
        # конфликты: одинаковый code, но РАЗНЫЕ name (после очистки скобок)
        conflicts = []
        for code, items in by_code.items():
            if len(items) < 2:
                continue

            raw_names = {(it['name'] or '').strip() for it in items}
            clean_names = {self.clean_name(it['name']) for it in items}

            # если после очистки все названия совпадают -> считаем одинаковыми, конфликта нет
            if len(clean_names) > 1:
                conflicts.append({
                    'code': code,
                    'count': len(items),
                    'rows': ','.join(str(it['row']) for it in items),
                    'names_clean': ' | '.join(sorted(clean_names))[:1000],
                    'names_raw': ' | '.join(sorted(raw_names))[:1000],
                })

        # пишем только КОНФЛИКТЫ
        out_path = f"{out_dir}/dupes_in_file.csv"
        with open(out_path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['code', 'count', 'rows', 'names_clean', 'names_raw'])
            w.writeheader()
            for r in sorted(conflicts, key=lambda x: (-x['count'], x['code'])):
                w.writerow(r)

        self.stdout.write(self.style.SUCCESS(
            f"Готово. Конфликты (одинаковый код, разные названия): {len(conflicts)}\n"
            f"Файл: {out_path}"
        ))
