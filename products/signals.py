from django.db.models.signals import pre_save
from django.dispatch import receiver
from .models import ProductImage

@receiver(pre_save, sender=ProductImage)
def ensure_single_main(sender, instance, **kwargs):
    if instance.is_main:
        (ProductImage.objects
         .filter(product=instance.product, is_main=True)
         .exclude(pk=instance.pk)
         .update(is_main=False))
