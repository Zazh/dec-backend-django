# products/management/commands/merge_price_to_unique.py
import csv
from decimal import Decimal, InvalidOperation
from django.core.management.base import BaseCommand, CommandError

class Command(BaseCommand):
    help = ("Склеивает цены из price.csv в unique.csv по Точному совпадению кода (строка после strip). "
            "Результаты: unique_with_price.csv и price_not_found_in_unique.csv")

    def add_arguments(self, p):
        p.add_argument('--unique', required=True, help='Путь к unique.csv (UTF-8). Должна быть колонка: код (или code).')
        p.add_argument('--price', required=True, help='Путь к price.csv (UTF-8). Должны быть: Код/Наименование/Цена: РРЦ (имена можно переопределить).')
        p.add_argument('--out-dir', default='.', help='Папка для результатов.')
        p.add_argument('--price-col', default='Цена: РРЦ', help='Имя ценовой колонки в price.csv.')
        p.add_argument('--code-col', default='Код', help='Имя колонки кода в price.csv (если не “Код”).')
        p.add_argument('--name-col', default='Наименование', help='Имя колонки названия в price.csv.')
        p.add_argument('--result-price-col', default='Цена: РРЦ', help='Имя ценовой колонки в выходном unique_with_price.csv.')

    # ---------- helpers ----------
    def _sniff(self, fobj):
        sample = fobj.read(4096); fobj.seek(0)
        try:
            return csv.Sniffer().sniff(sample, delimiters=[',',';','\t','|'])
        except csv.Error:
            # разумный фоллбек: в прайсах чаще всего ';'
            d = csv.get_dialect('excel')
            d.delimiter = ';'
            return d

    def _norm_key(self, s: str) -> str:
        return (s or '').replace('\ufeff', '').strip().lower()

    def _read_rows_and_header(self, path):
        with open(path, 'r', encoding='utf-8', newline='') as f:
            dialect = self._sniff(f)
            reader = csv.reader(f, dialect)
            rows = list(reader)
        if not rows:
            raise CommandError(f'Пустой CSV: {path}')
        header = { self._norm_key(h): idx for idx, h in enumerate(rows[0]) }
        return rows, header, dialect

    def _idx(self, header, *names):
        # Найти первый попавшийся индекс колонки из списка имён (учёт регистра/пробелов/BOM уже сделан в _norm_key)
        for n in names:
            i = header.get(self._norm_key(n))
            if i is not None:
                return i
        return None

    def _parse_price(self, s: str):
        if s is None:
            return ''
        s = s.strip()
        if not s:
            return ''
        # убираем пробелы/неразрывные пробелы в числе и приводим ',' к '.'
        s_clean = s.replace(' ', '').replace('\u00A0','').replace(',', '.')
        try:
            # валидируем, что это число; возвращаем строкой, чтобы не терять формат
            Decimal(s_clean)
            return s_clean
        except InvalidOperation:
            # если это не число (текст/пусто) — вернём исходник, чтобы не потерять инфу
            return s

    # ---------- main ----------
    def handle(self, *args, **o):
        unique_path = o['unique']; price_path = o['price']
        out_dir = o['out_dir'].rstrip('/')
        result_price_col = o['result_price_col']

        # 1) читаем unique.csv (с авто-разделителем)
        u_rows, u_header, u_dialect = self._read_rows_and_header(unique_path)
        u_cols = u_rows[0][:]

        # колонка кода в unique
        u_code_idx = self._idx(u_header, 'код', 'code')
        if u_code_idx is None:
            raise CommandError("В unique.csv не найдена колонка 'код' (или 'code').")

        # добавим/найдём колонку цены в выходе
        price_idx_out = self._idx(u_header, result_price_col)
        if price_idx_out is None:
            u_cols.append(result_price_col)
            price_idx_out = len(u_cols) - 1

        # 2) читаем price.csv
        p_rows, p_header, _ = self._read_rows_and_header(price_path)

        # гибко определим индексы нужных полей
        p_code_idx = self._idx(p_header, o['code_col'], 'код', 'code')
        if p_code_idx is None:
            raise CommandError(f"В price.csv не найдена колонка кода ('{o['code_col']}' / 'Код' / 'code').")

        p_name_idx = self._idx(p_header, o['name_col'], 'наименование', 'name')

        p_price_idx = self._idx(p_header, o['price_col'])
        if p_price_idx is None:
            raise CommandError(f"В price.csv не найдена колонка цены '{o['price_col']}'.")

        # 3) соберём мапу code -> price (берём ПОСЛЕДНЕЕ вхождение)
        price_map = {}
        for row in p_rows[1:]:
            code = (row[p_code_idx].strip() if p_code_idx < len(row) else '')
            if not code:
                continue
            price_val_raw = (row[p_price_idx].strip() if p_price_idx < len(row) else '')
            price_map[code] = self._parse_price(price_val_raw)

        # 4) пройдёмся по unique и добавим цену
        out_rows = [u_cols]
        seen_codes = set()
        for row in u_rows[1:]:
            # расширим строку, если добавили новую колонку
            if len(row) < len(u_cols):
                row = row + [''] * (len(u_cols) - len(row))
            code = (row[u_code_idx].strip() if u_code_idx < len(row) else '')
            if code and code in price_map:
                row[price_idx_out] = price_map[code]
                seen_codes.add(code)
            out_rows.append(row)

        # 5) позиции, которых нет в unique
        not_found = []
        for i, row in enumerate(p_rows[1:], start=2):
            code = (row[p_code_idx].strip() if p_code_idx < len(row) else '')
            if not code or code in seen_codes:
                continue
            name = (row[p_name_idx].strip() if (p_name_idx is not None and p_name_idx < len(row)) else '')
            price_val = (row[p_price_idx].strip() if p_price_idx < len(row) else '')
            not_found.append({'row': i, 'code': code, 'name': name, 'price': price_val})

        # 6) пишем результаты
        unique_out = f"{out_dir}/unique_with_price.csv"
        with open(unique_out, 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f, u_dialect)
            w.writerows(out_rows)

        nf_out = f"{out_dir}/price_not_found_in_unique.csv"
        with open(nf_out, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['row','code','name','price'])
            w.writeheader()
            for r in not_found:
                w.writerow(r)

        self.stdout.write(self.style.SUCCESS(
            f"Готово.\n"
            f" – unique_with_price.csv: {len(out_rows)-1} строк\n"
            f" – price_not_found_in_unique.csv: {len(not_found)} строк\n"
            f"Папка: {out_dir}"
        ))
