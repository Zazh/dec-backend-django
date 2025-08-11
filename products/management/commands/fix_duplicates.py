from django.core.management.base import BaseCommand
from products.models import ProductImage

class Command(BaseCommand):
    help = "Автоматически перепривязывает дубликаты и формирует bash-скрипт для их удаления."

    def handle(self, *args, **options):
        with open('./used_files.txt') as f:
            used_files = set(line.strip() for line in f)

        dupe_groups_path = './dupe_groups.txt'
        actions = []

        with open(dupe_groups_path) as f:
            group = []
            for line in f:
                line = line.strip()
                if line.startswith('Дубликаты для') or not line:
                    if len(group) > 1:
                        actions.append(list(group))
                    group = []
                else:
                    p = line[2:] if line.startswith('./') else line
                    group.append(p)
            if len(group) > 1:
                actions.append(list(group))

        # Открываем файл для bash-удаления дублей
        with open('delete_duplicates.sh', 'w') as out:
            out.write("#!/bin/bash\n\n")
            for group in actions:
                used = []
                not_used = []
                for x in group:
                    db_path = f"product_images/{x.lstrip('./')}"
                    if db_path in used_files:
                        used.append(x)
                    else:
                        not_used.append(x)
                # Перепривязываем все используемые дубли к оригиналу
                if len(used) > 1:
                    original_db_path = f"product_images/{used[0].lstrip('./')}"
                    for x in used[1:]:
                        dup_db_path = f"product_images/{x.lstrip('./')}"
                        self.stdout.write(self.style.WARNING(
                            f"Перепривязка {dup_db_path} -> {original_db_path}"))
                        ProductImage.objects.filter(image=dup_db_path).update(image=original_db_path)
                # Формируем команды на удаление неиспользуемых файлов
                for d in not_used:
                    cmd = f"rm -f media/product_images/{d}\n"
                    out.write(cmd)
                    self.stdout.write(self.style.NOTICE(cmd.strip()))

        self.stdout.write(self.style.SUCCESS("Файл delete_duplicates.sh создан. Запусти его на локальной машине для удаления дублей!"))
