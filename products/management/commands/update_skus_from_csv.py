import csv
from django.apps import apps
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

class Command(BaseCommand):
    help = (
        "Обновляет SKU в БД из отчёта assign_skus_by_db/assign_skus.\n"
        "По умолчанию применяет только статусы exact/plain_best. Есть --dry-run и два отчёта:\n"
        " - update_skus_report.csv (все строки с итогом would_update/ok/conflict/skip/error/noop)\n"
        " - update_skus_problems_products.csv (только проблемные, с названиями товаров и конфликтующих позиций)."
    )

    def add_arguments(self, p):
        p.add_argument("--file", required=True, help="Путь к CSV (например, sku_report_by_db.csv)")
        p.add_argument("--model", default="products.Product", help="app_label.ModelName (модель товара)")
        p.add_argument("--name-field", default="title", help="Поле названия товара в модели (для отчёта проблем)")
        p.add_argument("--sku-field", default="sku", help="Поле SKU в модели")
        p.add_argument("--id-col", default="product_id", help="Колонка с id товара в CSV")
        p.add_argument("--new-sku-col", default="new_sku", help="Колонка нового SKU в CSV")
        p.add_argument("--status-col", default="match_status", help="Колонка статуса (exact/plain_best/...) в CSV")
        p.add_argument("--allowed-statuses", default="exact,plain_best",
                       help="Какие статусы применять (через запятую). Пример: exact,plain_best,fuzzy_exact")
        p.add_argument("--dry-run", action="store_true", help="Показать, что бы обновили, без записи в БД")
        p.add_argument("--apply", action="store_true", help="Выполнить обновление в БД")
        p.add_argument("--using", default="default", help="Алиас базы (DATABASES)")
        p.add_argument("--report", default="update_skus_report.csv", help="Основной отчёт")
        p.add_argument("--problems-report", default="update_skus_problems_products.csv",
                       help="Отчёт по проблемным строкам с названиями товаров")

    # ---------- utils ----------
    def _sniff(self, fobj):
        sample = fobj.read(4096); fobj.seek(0)
        try:
            return csv.Sniffer().sniff(sample, delimiters=[",",";","\t","|"])
        except csv.Error:
            d = csv.get_dialect("excel"); d.delimiter = ","
            return d

    def _norm(self, s: str) -> str:
        return (s or "").replace("\ufeff","").strip().lower()

    def _write_report(self, path, rows):
        fields = ["row","product_id","old_sku","new_sku","result","reason"]
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(
                f, fieldnames=fields, delimiter=",", quotechar='"',
                quoting=csv.QUOTE_MINIMAL, escapechar="\\", lineterminator="\n"
            )
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in fields})

    def _write_problems(self, path, rows):
        fields = [
            "problem_type", "product_id", "product_title",
            "new_sku", "reason", "conflict_ids", "conflict_titles"
        ]
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(
                f, fieldnames=fields, delimiter=",", quotechar='"',
                quoting=csv.QUOTE_MINIMAL, escapechar="\\", lineterminator="\n"
            )
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in fields})

    # ---------- main ----------
    def handle(self, *args, **o):
        if not (o["dry_run"] ^ o["apply"]):
            raise CommandError("Нужно выбрать ровно один режим: --dry-run ИЛИ --apply")

        allowed_statuses = {s.strip() for s in (o["allowed_statuses"] or "").split(",") if s.strip()}
        if not allowed_statuses:
            allowed_statuses = {"exact", "plain_best"}

        Model = apps.get_model(o["model"])
        name_field = o["name_field"]
        sku_field = o["sku_field"]
        using = o["using"]

        # читаем CSV-план
        try:
            with open(o["file"], "r", encoding="utf-8", newline="") as f:
                dialect = self._sniff(f)
                reader = csv.reader(f, dialect)
                rows = list(reader)
        except FileNotFoundError:
            raise CommandError(f"Файл не найден: {o['file']}")
        if not rows:
            raise CommandError("Пустой CSV")

        header = { self._norm(h): i for i, h in enumerate(rows[0]) }
        def get(row, *names):
            for n in names:
                idx = header.get(self._norm(n))
                if idx is not None and idx < len(row):
                    return (row[idx] or "").strip()
            return ""

        id_col = o["id_col"]; new_col = o["new_sku_col"]; status_col = o["status_col"]

        # индекс текущей БД: sku -> ids, и id -> title
        current_sku_index = {}
        id_to_title = {}
        for obj in Model.objects.using(using).all().values("id", name_field, sku_field):
            v = (obj.get(sku_field) or "").strip()
            if v:
                current_sku_index.setdefault(v, set()).add(obj["id"])
            id_to_title[obj["id"]] = obj.get(name_field) or ""

        report_rows = []
        problems = []
        to_update = []   # (pk, new_sku, old_sku)

        # локальный индекс запрашиваемых new_sku в рамках текущего прогона
        planned_sku_claims = {}

        for i, row in enumerate(rows[1:], start=2):
            pid_raw = get(row, id_col)
            status  = get(row, status_col)
            new_sku = get(row, new_col)

            # базовые фильтры
            if not pid_raw or not new_sku:
                report_rows.append({
                    "row": i, "product_id": pid_raw, "old_sku": "", "new_sku": new_sku,
                    "result": "skip", "reason": "missing product_id or new_sku"
                })
                problems.append({
                    "problem_type": "skip",
                    "product_id": pid_raw, "product_title": id_to_title.get(int(pid_raw)) if pid_raw.isdigit() else "",
                    "new_sku": new_sku, "reason": "missing product_id or new_sku",
                    "conflict_ids": "", "conflict_titles": ""
                })
                continue

            if status not in allowed_statuses:
                report_rows.append({
                    "row": i, "product_id": pid_raw, "old_sku": "", "new_sku": new_sku,
                    "result": "skip", "reason": f"status '{status}' not allowed"
                })
                problems.append({
                    "problem_type": "skip",
                    "product_id": pid_raw, "product_title": id_to_title.get(int(pid_raw)) if pid_raw.isdigit() else "",
                    "new_sku": new_sku, "reason": f"status '{status}' not allowed",
                    "conflict_ids": "", "conflict_titles": ""
                })
                continue

            # существует ли товар?
            try:
                obj = Model.objects.using(using).get(pk=pid_raw)
            except Model.DoesNotExist:
                report_rows.append({
                    "row": i, "product_id": pid_raw, "old_sku": "", "new_sku": new_sku,
                    "result": "error", "reason": "product not found"
                })
                problems.append({
                    "problem_type": "error",
                    "product_id": pid_raw, "product_title": "",
                    "new_sku": new_sku, "reason": "product not found",
                    "conflict_ids": "", "conflict_titles": ""
                })
                continue

            old_sku = (getattr(obj, sku_field) or "").strip()

            # если уже такой же sku — noop
            if old_sku == new_sku:
                report_rows.append({
                    "row": i, "product_id": pid_raw, "old_sku": old_sku, "new_sku": new_sku,
                    "result": "noop", "reason": "already set"
                })
                # это не проблема, но пусть будет видимо в основном отчёте
                continue

            # конфликт 1: занят в БД другим товаром
            owners = current_sku_index.get(new_sku, set())
            owners_without_self = owners - {obj.id}
            if owners_without_self:
                conflict_titles = [id_to_title.get(pid, "") for pid in sorted(owners_without_self)]
                report_rows.append({
                    "row": i, "product_id": pid_raw, "old_sku": old_sku, "new_sku": new_sku,
                    "result": "conflict", "reason": f"sku used by {sorted(owners_without_self)}"
                })
                problems.append({
                    "problem_type": "conflict",
                    "product_id": pid_raw, "product_title": id_to_title.get(obj.id, ""),
                    "new_sku": new_sku,
                    "reason": f"sku used by {sorted(owners_without_self)}",
                    "conflict_ids": ";".join(str(x) for x in sorted(owners_without_self)),
                    "conflict_titles": " | ".join(conflict_titles),
                })
                continue

            # конфликт 2: другой товар в ЭТОМ ЖЕ запуске уже запросил этот new_sku
            planned_by = planned_sku_claims.get(new_sku)
            if planned_by and planned_by != obj.id:
                # найдём имя конкурента (из БД)
                competitor_title = id_to_title.get(planned_by, "")
                report_rows.append({
                    "row": i, "product_id": pid_raw, "old_sku": old_sku, "new_sku": new_sku,
                    "result": "conflict", "reason": f"new_sku already planned by {planned_by}"
                })
                problems.append({
                    "problem_type": "conflict",
                    "product_id": pid_raw, "product_title": id_to_title.get(obj.id, ""),
                    "new_sku": new_sku,
                    "reason": f"new_sku already planned by {planned_by}",
                    "conflict_ids": str(planned_by),
                    "conflict_titles": competitor_title,
                })
                continue

            # планируем к обновлению
            to_update.append((obj.id, new_sku, old_sku))
            planned_sku_claims[new_sku] = obj.id

            # и сразу обновим локальный индекс — чтобы дальше ловить конфликты корректно
            current_sku_index.setdefault(new_sku, set()).add(obj.id)
            if old_sku:
                s = current_sku_index.get(old_sku)
                if s and obj.id in s:
                    s.remove(obj.id)

        # применение
        updated = 0
        if o["apply"]:
            with transaction.atomic(using=using):
                for pk, new_sku, old_sku in to_update:
                    Model.objects.using(using).filter(pk=pk).update(**{sku_field: new_sku})
                    updated += 1

        # собрать основной отчёт: все planned как ok/would_update (если ещё не были помечены)
        already = {(r.get("product_id"), r.get("new_sku")) for r in report_rows}
        for pk, new_sku, old_sku in to_update:
            key = (str(pk), new_sku)
            if key in already:
                continue
            report_rows.append({
                "row": "", "product_id": str(pk), "old_sku": old_sku, "new_sku": new_sku,
                "result": ("ok" if o["apply"] else "would_update"),
                "reason": ""
            })

        # записать отчёты
        self._write_report(o["report"], report_rows)
        self._write_problems(o["problems_report"], problems)

        if o["apply"]:
            self.stdout.write(self.style.SUCCESS(
                f"Готово. Обновлено: {updated}.\n"
                f"Отчёты:\n - {o['report']}\n - {o['problems_report']}"
            ))
        else:
            self.stdout.write(self.style.SUCCESS(
                f"DRY-RUN. Обновили бы: {len(to_update)}.\n"
                f"Отчёты:\n - {o['report']}\n - {o['problems_report']}"
            ))
