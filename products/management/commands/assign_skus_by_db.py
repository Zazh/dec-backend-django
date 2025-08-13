# products/management/commands/assign_skus_by_db.py
import csv, re
from collections import defaultdict, Counter
from django.apps import apps
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

# -------- utils (ТОЛЬКО для имен, не для кода) --------
CYR_TO_LAT = str.maketrans({
    'А':'A','В':'B','Е':'E','К':'K','М':'M','Н':'H','О':'O','Р':'P','С':'S','Т':'T','У':'Y','Х':'X',
    'а':'A','в':'B','е':'E','к':'K','м':'M','н':'H','о':'O','р':'P','с':'S','т':'T','у':'Y','х':'X',
})

def norm_code_like_for_name(s: str) -> str:
    """Нормализация ТОЛЬКО для головного кода из названия (кириллица->латиница, A-Z0-9)."""
    s = (s or '').strip().translate(CYR_TO_LAT).upper()
    return re.sub(r'[^A-Z0-9]+', '', s)

def strip_parens(s: str) -> str:
    if not s:
        return ""
    prev = None
    out = s
    while out != prev:
        prev = out
        out = re.sub(r"\([^()]*\)", "", out)
    return " ".join(out.split()).strip()

CODE_AT_START = re.compile(r'^\s*([A-Za-zА-Яа-я0-9\-]+)')
def head_from_name(name: str) -> str:
    m = CODE_AT_START.search(name or '')
    return norm_code_like_for_name(m.group(1)) if m else ''

DIM_RE = re.compile(r'(\d+)\s*[*xх]\s*(\d+)(?:\s*[*xх]\s*(\d{3,4}))?')
def extract_dims(text: str):
    m = DIM_RE.search(text or '')
    return m.groups('') if m else ()

DEFAULT_DENY_PATTERN = r'\bFLEX\b|^\s*[УU]\d+-|\bРАСПРОДАЖА\b'

class Command(BaseCommand):
    help = "Обходит товары БД, находит пару по НАЗВАНИЮ в input и ставит 'Код' как sku. Поиск ТОЛЬКО по названию."

    def add_arguments(self, p):
        p.add_argument('--file', required=True, help='input_clean.csv (UTF-8). Колонки: Код, Наименование/Название.')
        p.add_argument('--model', default='products.Product', help='app_label.ModelName')
        p.add_argument('--name-field', default='title', help='Поле названия товара в БД')
        p.add_argument('--sku-field', default='sku', help='Поле SKU для записи')
        p.add_argument('--deny-regex', default=DEFAULT_DENY_PATTERN, help='Regex для выкидывания шумных названий')
        p.add_argument('--dry-run', action='store_true')
        p.add_argument('--apply', action='store_true')
        p.add_argument('--only-safe', action='store_true', help='Применять только exact/plain_best')
        p.add_argument('--using', default='default')
        p.add_argument('--report', default='sku_report_by_db.csv')
        p.add_argument('--unused-report', default='input_unused.csv')
        p.add_argument('--not-covered-report', default='db_not_covered.csv')
        p.add_argument('--debug-candidates', action='store_true')
        p.add_argument('--code-col', default='Код', help='Имя колонки кода в input.csv')
        p.add_argument('--name-col', default='Наименование', help='Имя колонки названия в input.csv')
        p.add_argument('--debug-headers', action='store_true')

    # ---- CSV helpers ----
    def _sniff(self, fobj):
        sample = fobj.read(4096); fobj.seek(0)
        try:
            return csv.Sniffer().sniff(sample, delimiters=[',',';','\t','|'])
        except csv.Error:
            d = csv.get_dialect('excel'); d.delimiter = ';'
            return d

    def _norm_key(self, s: str) -> str:
        return (s or '').replace('\ufeff','').strip().lower()

    def _pick_idx(self, header, *keys):
        for k in keys:
            idx = header.get(self._norm_key(k))
            if idx is not None:
                return idx
        return None

    def _read_input(self, path, code_col, name_col, debug_headers=False):
        with open(path, 'r', encoding='utf-8', newline='') as f:
            dialect = self._sniff(f)
            reader = csv.reader(f, dialect)
            rows = list(reader)
        if not rows:
            raise CommandError('Пустой CSV')

        header = { self._norm_key(h): i for i, h in enumerate(rows[0]) }
        code_idx = self._pick_idx(header, code_col, 'Код', 'code')
        name_idx = self._pick_idx(header, name_col, 'Наименование', 'Название', 'name')

        if debug_headers:
            print("Header map:", header)
            print("Detected code_idx:", code_idx, "name_idx:", name_idx)

        if code_idx is None or name_idx is None:
            raise CommandError("Нужны колонки 'Код' и 'Наименование/Название' в input (или укажи --code-col/--name-col).")

        items = []
        for i, r in enumerate(rows[1:], start=2):
            code_raw = (r[code_idx].strip() if code_idx < len(r) else '')
            name = (r[name_idx].strip() if name_idx < len(r) else '')
            if not code_raw and not name:
                continue
            name_clean = strip_parens(name)
            items.append({
                'row': i,
                'excel_code': code_raw,             # КОД — только для записи, не для поиска
                'excel_name': name,
                'excel_name_clean': name_clean,     # для поиска
                'head_excel': head_from_name(name_clean),
                'dims_excel': extract_dims(name_clean),
            })
        return items, rows[0], dialect

    # ---- writers ----
    def _write_rows(self, path, rows, delimiter=','):
        with open(path, 'w', encoding='utf-8', newline='') as f:
            w = csv.writer(
                f, delimiter=delimiter, quotechar='"',
                quoting=csv.QUOTE_MINIMAL, escapechar='\\',
                lineterminator='\n', doublequote=True,
            )
            w.writerows(rows)

    def _write_dicts(self, path, fields, dict_rows):
        with open(path, 'w', encoding='utf-8', newline='') as f:
            w = csv.DictWriter(
                f, fieldnames=fields, delimiter=',', quotechar='"',
                quoting=csv.QUOTE_MINIMAL, escapechar='\\', lineterminator='\n'
            )
            w.writeheader()
            for r in dict_rows:
                w.writerow({k: r.get(k, '') for k in fields})

    # ---- main ----
    def handle(self, *a, **o):
        if not (o['dry_run'] ^ o['apply']):
            raise CommandError('Нужно выбрать ровно один режим: --dry-run ИЛИ --apply')

        using = o['using']
        deny_re = re.compile(o['deny_regex'], flags=re.IGNORECASE)

        Model = apps.get_model(o['model'])
        name_f, sku_f = o['name_field'], o['sku_field']

        # 1) input
        items, input_header, input_dialect = self._read_input(
            o['file'], o['code_col'], o['name_col'], o['debug_headers']
        )

        # дубли КОДОВ в input — по СЫРОМУ коду (строгое равенство)
        code_counts = Counter(it['excel_code'] for it in items if it['excel_code'])
        dup_codes = {k for k, v in code_counts.items() if v > 1}

        # индекс по головам из названия
        by_head = defaultdict(list)
        by_digits = defaultdict(list)

        for it in items:
            if it['head_excel']:
                by_head[it['head_excel']].append(it)
                digits = re.sub(r'[A-Z]+', '', it['head_excel'])  # оставить только цифры
                if digits:
                    by_digits[digits].append(it)

        # 2) продукты из БД
        qs = (Model.objects.using(using).all().values('id', name_f, *( [sku_f] if sku_f else [] )))
        products = []
        for p in qs:
            nm = p[name_f] or ''
            nm_clean = strip_parens(nm)
            p['head_db'] = head_from_name(nm_clean)
            p['dims_db']  = extract_dims(nm_clean)
            p['nm_clean'] = nm_clean
            products.append(p)

        # кто уже владеет каким КОДОМ (sku)
        sku_owner = {}
        for p in products:
            old = (p.get(sku_f) or '').strip()
            if old:
                sku_owner.setdefault(old, []).append(p['id'])

        # 3) матчинг: БД → input (Только по названиям)
        used_input_rows = set()
        results = []
        for p in products:
            head = p['head_db']
            candidates = list(by_head.get(head, [])) if head else []

            # fallback 1: БД-голова длиннее/короче на буквенный суффикс
            if not candidates and head:
                for key, lst in by_head.items():
                    if head.startswith(key):
                        extra = head[len(key):]
                        if 1 <= len(extra) <= 3 and extra.isalpha():
                            candidates.extend(lst)
                if not candidates:
                    for key, lst in by_head.items():
                        if key.startswith(head):
                            extra = key[len(head):]
                            if 1 <= len(extra) <= 3 and extra.isalpha():
                                candidates.extend(lst)

            # fallback 2: Сведение по цифрам без букв (00460 == 004G60)
            if not candidates and head:
                digits = re.sub(r'[A-Z]+', '', head)
                if digits:
                    candidates.extend(by_digits.get(digits, []))

            # выкинем шумные названия из кандидатов
            filtered = [it for it in candidates if not deny_re.search(it['excel_name'].upper())]
            if filtered:
                candidates = filtered

            def score(it):
                s = 0
                he = it['head_excel']
                if he == head:
                    s = 3
                elif head and he and (head.startswith(he) or he.startswith(head)):
                    s = 2
                else:
                    # новый бонус: совпадают цифры без букв
                    if re.sub(r'[A-Z]+', '', he) == re.sub(r'[A-Z]+', '', head):
                        s = 2
                if it['dims_excel'] and p['dims_db'] and set(it['dims_excel']) & set(p['dims_db']):
                    s += 1
                return s

            status, reason, choice = 'not_found', 'no candidates', None
            if candidates:
                candidates.sort(key=lambda it: (score(it), -len(it['excel_name_clean'] or '')), reverse=True)
                best_score = score(candidates[0])
                best = [it for it in candidates if score(it) == best_score]
                best.sort(key=lambda it: len(it['excel_name_clean'] or ''))
                choice = best[0]
                if best_score >= 2:
                    status = 'exact' if best_score >= 3 else 'plain_best'
                    reason = f'{len(candidates)} cand, score={best_score}'
                else:
                    status, reason = 'ambiguous', f'{len(candidates)} candidates, best_score={best_score}'

            new_sku = (choice['excel_code'].strip() if choice else '')

            # безопасность
            if new_sku and new_sku in dup_codes:
                status, reason = 'duplicate_in_input', 'this code appears multiple times in input'
            if new_sku:
                owners = sku_owner.get(new_sku, [])
                if owners and p['id'] not in owners:
                    status, reason = 'db_sku_taken', f'sku already used by product ids {owners}'

            row = {
                'product_id': p['id'],
                'product_name': p[name_f],
                'old_sku': p.get(sku_f, ''),
                'match_status': status,
                'reason': reason,
                'new_sku': new_sku,
                'excel_row': (choice['row'] if choice else ''),
                'excel_code': (choice['excel_code'] if choice else ''),
                'excel_name': (choice['excel_name'] if choice else ''),
            }
            if o['debug_candidates']:
                row['candidates'] = '; '.join(f"{c['row']}|{c['excel_name']}" for c in candidates[:10])
            results.append(row)

            if choice and status in {'exact','plain_best'}:
                used_input_rows.add(choice['row'])

        # 4) отчёты
        summary = Counter(r['match_status'] for r in results)
        covered = sum(1 for r in results if r['match_status'] in {'exact','plain_best'})
        self.stdout.write('Coverage: ' + ', '.join(f'{k}={v}' for k, v in summary.items()))
        self.stdout.write(f'Covered {covered} of {len(results)} products ({covered*100//max(1,len(results))}%).')

        fields = ['product_id','product_name','old_sku','match_status','reason','new_sku',
                  'excel_row','excel_code','excel_name']
        if o['debug_candidates']:
            fields.append('candidates')
        self._write_dicts(o['report'], fields, results)

        # input-строки, которые ни разу не использовались
        unused = [it for it in items if it['row'] not in used_input_rows]
        unused_rows = [['row','code','name']]
        unused_rows += [[u['row'], u['excel_code'], u['excel_name']] for u in unused]
        self._write_rows(o['unused_report'], unused_rows, delimiter=input_dialect.delimiter)

        # товары БД без уверенной пары
        not_cov = [r for r in results if r['match_status'] not in {'exact','plain_best'}]
        self._write_dicts(o['not_covered_report'],
                          ['product_id','product_name','old_sku','match_status','reason'],
                          not_cov)

        if o['dry_run']:
            self.stdout.write(self.style.SUCCESS(
                f"DRY-RUN. Отчёты:\n - {o['report']}\n - {o['unused_report']}\n - {o['not_covered_report']}"
            ))
            return

        # 5) APPLY
        safe = {'exact','plain_best'} if o['only_safe'] else {'exact','plain_best'}
        to_apply = [r for r in results if r['match_status'] in safe and r['new_sku']]

        updated = 0
        with transaction.atomic(using=using):
            for r in to_apply:
                obj = Model.objects.using(using).select_for_update().get(pk=r['product_id'])
                new_val = r['new_sku'].strip()
                if getattr(obj, sku_f) != new_val:
                    setattr(obj, sku_f, new_val)
                    obj.save(update_fields=[sku_f])
                    updated += 1

        self.stdout.write(self.style.SUCCESS(f'Обновлено: {updated}. Отчёт: {o["report"]}'))
