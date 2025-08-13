# products/management/commands/clean_names_parentheses.py
import csv, re
from django.core.management.base import BaseCommand, CommandError

def strip_parens(s: str) -> str:
    if not s:
        return ""
    # Многоразово вырезаем (...), чтобы убрать все вхождения
    prev = None
    out = s
    while out != prev:
        prev = out
        out = re.sub(r"\([^()]*\)", "", out)
    # нормализуем пробелы
    out = " ".join(out.split())
    return out.strip()

class Command(BaseCommand):
    help = "Создаёт копию CSV, где в колонке Наименование/Название удалены круглые скобки и содержимое."

    def add_arguments(self, p):
        p.add_argument('--file', required=True, help='Путь к исходному CSV (UTF-8). Колонки: Код и Наименование/Название.')
        p.add_argument('--out', required=True, help='Путь к очищенному CSV (будет создан).')

    def _sniff(self, fobj):
        sample = fobj.read(4096); fobj.seek(0)
        try:
            return csv.Sniffer().sniff(sample, delimiters=[',',';','\t','|'])
        except csv.Error:
            d = csv.get_dialect('excel'); d.delimiter = ';'  # частый случай для прайсов
            return d

    def _norm(self, s: str) -> str:
        return (s or '').replace('\ufeff','').strip().lower()

    def handle(self, *args, **o):
        src = o['file']; dst = o['out']

        # читаем
        try:
            with open(src, 'r', encoding='utf-8', newline='') as f:
                dialect = self._sniff(f)
                reader = csv.reader(f, dialect)
                rows = list(reader)
        except FileNotFoundError:
            raise CommandError(f'Файл не найден: {src}')
        if not rows:
            raise CommandError('Пустой CSV')

        header = { self._norm(h): i for i, h in enumerate(rows[0]) }
        # поддержим разные варианты имен колонки
        name_idx = header.get('наименование') or header.get('название') or header.get('name')
        if name_idx is None:
            raise CommandError("Не найдена колонка 'Наименование'/'Название'/'name'.")

        # очищаем каждую строку
        out_rows = [rows[0]]
        for r in rows[1:]:
            if name_idx < len(r):
                r = r[:]  # копия
                r[name_idx] = strip_parens(r[name_idx])
            out_rows.append(r)

        # пишем
        with open(dst, 'w', encoding='utf-8', newline='') as f:
            w = csv.writer(
                f,
                delimiter=dialect.delimiter,
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL,
                escapechar='\\',
                lineterminator='\n',
                doublequote=True,
            )
            w.writerows(out_rows)

        self.stdout.write(self.style.SUCCESS(f"Готово. Очищенный файл: {dst}"))
