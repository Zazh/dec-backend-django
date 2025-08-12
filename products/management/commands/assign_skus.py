# products/management/commands/assign_skus.py
import csv, re
from collections import defaultdict, Counter

from django.apps import apps
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

# ---- нормализация --------------------------------------------------------
CYR_TO_LAT = str.maketrans({
    'А':'A','В':'B','Е':'E','К':'K','М':'M','Н':'H','О':'O','Р':'P','С':'S','Т':'T','У':'Y','Х':'X',
    'а':'A','в':'B','е':'E','к':'K','м':'M','н':'H','о':'O','р':'P','с':'S','т':'T','у':'Y','х':'X',
})

def norm_code(s: str) -> str:
    s = (s or '').strip().translate(CYR_TO_LAT).upper()
    return re.sub(r'[^A-Z0-9]+', '', s)

CODE_AT_START = re.compile(r'^\s*([A-Za-zА-Яа-я0-9\-]+)')
def head_from_name(name: str) -> str:
    m = CODE_AT_START.search(name or '')
    return norm_code(m.group(1)) if m else ''

DIM_RE = re.compile(r'(\d+)\s*[*xх]\s*(\d+)(?:\s*[*xх]\s*(\d{3,4}))?')
def extract_dims(text: str):
    m = DIM_RE.search(text or '')
    return m.groups('') if m else ()

# по умолчанию режем FLEX и префиксы вида У30- / U30-
DEFAULT_DENY_PATTERN = r'\bFLEX\b|^\s*[УU]\d+-'

# ---- команда -------------------------------------------------------------
class Command(BaseCommand):
    help = "Назначение реальных артикулов (SKU) товарам из CSV без использования категорий. Поддерживает dry-run."

    def add_arguments(self, p):
        p.add_argument('--file', required=True, help='Путь к CSV (UTF-8). Колонки: код, название, Тип (Тип можно игнорить).')
        p.add_argument('--model', default='products.Product', help='app_label.ModelName товара')
        p.add_argument('--name-field', default='title', help='Поле названия товара')
        p.add_argument('--sku-field', default='sku', help='Поле артикула для записи')
        p.add_argument('--deny-regex', default=DEFAULT_DENY_PATTERN, help='Regex для исключения шумных вариантов')
        # режимы
        p.add_argument('--dry-run', action='store_true')
        p.add_argument('--apply', action='store_true')
        p.add_argument('--only-safe', action='store_true', help='Применять только exact/plain_best')
        p.add_argument('--using', default='default', help='Алиас базы')
        p.add_argument('--report', default='sku_report.csv')
        p.add_argument('--debug-candidates', action='store_true', help='Добавить колонку candidates в отчёт')

    # ------------------------------ utils --------------------------------
    def _read_csv_items(self, path):
        def _norm_key(k: str) -> str:
            return (k or '').replace('\ufeff', '').strip().lower()

        with open(path, 'r', encoding='utf-8', newline='') as f:
            sample = f.read(4096); f.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=[',',';','\t'])
            except csv.Error:
                dialect = csv.get_dialect('excel'); dialect.delimiter = ','
            reader = csv.reader(f, dialect)
            rows_raw = list(reader)
            if not rows_raw:
                raise CommandError('Пустой файл CSV')

            header = {_norm_key(h): idx for idx, h in enumerate(rows_raw[0])}

            def col(row, name, alt=None):
                idx = header.get(name) or (header.get(alt) if alt else None)
                return (row[idx].strip() if idx is not None and idx < len(row) else '')

            items = []
            for i, row in enumerate(rows_raw[1:], start=2):
                excel_code = col(row, 'код', 'code')
                excel_name = col(row, 'название', 'name')
                excel_type = col(row, 'тип', 'type')  # можем не использовать
                new_sku = excel_code
                items.append({
                    'row': i,
                    'excel_code': excel_code,
                    'excel_name': excel_name,
                    'excel_type': excel_type,
                    'new_sku': new_sku,
                    'new_sku_norm': norm_code(new_sku),
                    'head_excel': head_from_name(excel_name) or norm_code(new_sku),
                    'dims_excel': extract_dims(excel_name),
                })
            return items

    # ------------------------------ handle -------------------------------
    def handle(self, *a, **o):
        if not (o['dry_run'] ^ o['apply']):
            raise CommandError('Нужно выбрать ровно один режим: --dry-run ИЛИ --apply')

        using = o['using']
        deny_re = re.compile(o['deny_regex'], flags=re.IGNORECASE)

        Model = apps.get_model(o['model'])
        name_f, sku_f = o['name_field'], o['sku_field']

        # 0) читаем CSV
        items = self._read_csv_items(o['file'])

        # 0.1) дубль новых SKU в самом файле
        cnt = Counter(x['new_sku_norm'] for x in items if x['new_sku_norm'])
        dupes = {k for k, v in cnt.items() if v > 1}

        # 1) индексы по товарам
        qs = (Model.objects.using(using)
              .all()
              .values('id', name_f, *( [sku_f] if sku_f else [] )))

        by_head = defaultdict(list)
        products = []
        for p in qs:
            nm = p[name_f] or ''
            p['head'] = head_from_name(nm)
            p['dims'] = extract_dims(nm)
            by_head[p['head']].append(p)
            products.append(p)

        # 2) матчинг
        results = []
        for it in items:
            status, reason, prod = 'not_found', 'no candidates', None
            candidates = [p for p in by_head.get(it['head_excel'], [])]

            # сначала пытаемся выкинуть шумные варианты; если всё выпилили — вернём исходный список
            filtered = [p for p in candidates if not deny_re.search((p[name_f] or '').upper())]
            if filtered:
                candidates = filtered

            # скоринг
            def score(p):
                nm = (p[name_f] or '').strip()
                nm_up = nm.upper()
                nm_norm = norm_code(nm)  # <-- добавили

                head = it['head_excel']


                # 3 — имя ровно равно коду (или код + пунктуация)
                if nm_up == head or re.fullmatch(rf'\s*{re.escape(head)}\s*[\.\,/–-]*\s*', nm_up):
                    return 3

                # 2/3 — начинается с кода (не попало под deny); +1 за совпадение размеров
                if nm_up.startswith(head) and not deny_re.search(nm_up):
                    bonus = 1 if (it['dims_excel'] and p['dims'] and set(p['dims']) & set(it['dims_excel'])) else 0
                    return 2 + bonus

                return 0

            if candidates:
                candidates.sort(key=lambda p: (score(p), -len(p[name_f] or '')), reverse=True)
                top_score = score(candidates[0])
                best = [p for p in candidates if score(p) == top_score]

                # если среди лучших несколько — предпочитаем самое короткое имя (скорее «чистый» код)
                best.sort(key=lambda p: len(p[name_f] or ''))
                chosen = best[0] if best else None

                if chosen and top_score >= 2:
                    status = 'exact' if top_score >= 3 else 'plain_best'
                    reason = f'{len(candidates)} cand, score={top_score}'
                    prod = chosen
                else:
                    status, reason = 'ambiguous', f'{len(candidates)} candidates, best_score={top_score}'

            # дубль нового SKU в файле
            if it['new_sku_norm'] in dupes:
                status, reason, prod = 'duplicate_new_sku', 'new SKU duplicated in file', prod

            # занято ли такое SKU уже в БД (другим товаром)?
            if it['new_sku']:
                q = Model.objects.using(using).filter(**{sku_f: it['new_sku'].strip()})
                if prod:
                    q = q.exclude(pk=prod['id'])
                if q.exists():
                    status, reason = 'db_sku_taken', f'{sku_f} already used by another product'

            row = {
                **it,
                'match_status': status,
                'reason': reason,
                'product_id': prod['id'] if prod else '',
                'product_name': prod[name_f] if prod else '',
                'old_sku': prod.get(sku_f, '') if prod else '',
                'applied': '',
            }
            if o['debug_candidates']:
                row['candidates'] = '; '.join(f"{c['id']}|{c[name_f]}" for c in candidates[:10])
            results.append(row)

        # сводка
        summary = Counter(r['match_status'] for r in results)
        self.stdout.write('Summary: ' + ', '.join(f'{k}={v}' for k, v in summary.items()))

        # 3) DRY-RUN?
        if o['dry_run']:
            self._write(o['report'], results, debug=o['debug_candidates'])
            self.stdout.write(self.style.SUCCESS(f'DRY-RUN готов. Отчёт: {o["report"]}'))
            return

        # 4) APPLY
        safe = {'exact', 'plain_best'} if o['only_safe'] else {'exact', 'plain_best'}
        to_apply = [r for r in results if r['match_status'] in safe and r['product_id'] and r['new_sku']]

        updated = 0
        with transaction.atomic(using=using):
            for r in to_apply:
                obj = Model.objects.using(using).select_for_update().get(pk=r['product_id'])
                new_val = r['new_sku'].strip()
                if getattr(obj, sku_f) != new_val:
                    setattr(obj, sku_f, new_val)
                    obj.save(update_fields=[sku_f])
                    updated += 1
                r['applied'] = 'yes'

        self._write(o['report'], results, debug=o['debug_candidates'])
        self.stdout.write(self.style.SUCCESS(f'Обновлено: {updated}. Отчёт: {o["report"]}'))

    def _write(self, path, rows, debug=False):
        fields = [
            'row','match_status','reason','product_id','product_name',
            'old_sku','new_sku','excel_code','excel_name','excel_type','head_excel','applied'
        ]
        if debug:
            fields.append('candidates')
        with open(path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, '') for k in fields})
