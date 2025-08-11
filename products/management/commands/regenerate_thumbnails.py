from django.core.management.base import BaseCommand
from easy_thumbnails.files import get_thumbnailer
from products.models import ProductImage
from django.conf import settings

class Command(BaseCommand):
    help = "Re-generate thumbnails for a range of ProductImages"

    def add_arguments(self, parser):
        parser.add_argument('--start', type=int, default=0, help='Start index (0-based)')
        parser.add_argument('--end', type=int, default=None, help='End index (exclusive)')

    def handle(self, *args, **options):
        start = options['start']
        end = options['end']
        images = ProductImage.objects.all().order_by('id')
        if end is not None:
            images = images[start:end]
        else:
            images = images[start:start+1]

        total = images.count()
        self.stdout.write(f"Обрабатываем {total} изображений…")
        for i, img in enumerate(images, start+1):
            thumb = get_thumbnailer(img.image)
            thumb.delete_thumbnails()
            thumb.get_thumbnail(settings.THUMBNAIL_ALIASES[""]["default"])
            thumb.get_thumbnail(settings.THUMBNAIL_ALIASES[""]["preview"])
            self.stdout.write(f"[{i}] {img.image.name}")
        self.stdout.write(self.style.SUCCESS("Готово!"))
