import os
import random
import string
import uuid
from slugify import slugify as _slugify

# ────────── единый slugify ──────────
def slug(text: str, max_len: int = 50) -> str:
    # если text пустой — делаем dummy-значение
    base = _slugify(text, lowercase=True, max_length=max_len, separator='-') or "item"
    return base

def unique_slug(instance, text, model, field_name="slug", max_len=50, suffix_len=4):
    base = slug(text, max_len=max_len-suffix_len-1)
    candidate = base
    while model.objects.exclude(pk=getattr(instance, "pk", None)).filter(**{field_name: candidate}).exists():
        rand_suffix = ''.join(random.choices(string.digits, k=suffix_len))
        candidate = f"{base}-{rand_suffix}"
    return candidate

# ─────────── upload path ────────────
def product_image_upload_path(instance, filename):
    ext = filename.split('.')[-1].lower()
    product_slug = slug(instance.product.title)
    unique = uuid.uuid4().hex[:8]
    fname = f"{product_slug}-{unique}.{ext}"
    return os.path.join("product_images", product_slug, fname)

def category_image_upload_path(instance, filename):
    """ media/category_images/<slug>/uuid.ext """
    import uuid, os
    ext = filename.split('.')[-1]
    name = f"{instance.slug}-{uuid.uuid4().hex[:8]}.{ext}"
    return os.path.join("category_images", instance.slug, name)