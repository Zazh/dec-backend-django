# products/filters.py
import django_filters as df
from django.db.models import (
    DecimalField, OuterRef, Subquery,
)
from django.db.models.functions import Cast

from .models import (
    Product,
    Attribute,
    ProductAttributeValue,
)

# ────────────────────────────────────────────────────────────────
# Получаем slug-и основных атрибутов только один раз
def _slug(attr_name: str) -> str | None:
    try:
        return Attribute.objects.only("slug").get(
            name__iexact=attr_name
        ).slug
    except Attribute.DoesNotExist:
        return None


def get_attr_slug(dim: str) -> str | None:
    return _slug({
        "length": "длина",
        "width": "ширина",
        "height": "высота",
    }[dim])
# ────────────────────────────────────────────────────────────────


class ProductFilter(df.FilterSet):
    # ─ цена ──────────────────────────────────────────────────────
    price = df.RangeFilter(field_name="price")

    # ─ диапазоны по размерам (min / max) ─────────────────────────
    length_min  = df.NumberFilter(method="min_filter")
    length_max  = df.NumberFilter(method="max_filter")

    width_min   = df.NumberFilter(method="min_filter")
    width_max   = df.NumberFilter(method="max_filter")

    height_min  = df.NumberFilter(method="min_filter")
    height_max  = df.NumberFilter(method="max_filter")

    # ─ сортировка с фронта (ordering=-price …) ───────────────────
    ordering = df.OrderingFilter(fields=(("price", "price"),
                                         ("title", "title")))

    # ======== вспомогательные ========
    def _annotate_dim(self, qs, dim_alias: str, attr_slug: str):
        """
        Добавляет к QuerySet аннотацию <dim_alias> (Decimal) с числовым
        значением нужного атрибута. Выполняется ровно один раз.
        """
        if dim_alias in qs.query.annotations:
            return qs        # уже есть

        sub = Subquery(
            ProductAttributeValue.objects
            .filter(product=OuterRef("pk"), attribute__slug=attr_slug)
            .values("value")[:1]
        )

        return qs.annotate(**{
            dim_alias: Cast(sub, DecimalField(max_digits=12, decimal_places=2))
        })

    # ======== универсальные методы ========
    def min_filter(self, qs, name, value):
        dim = name.removesuffix("_min")  # length / width / height
        slug = get_attr_slug(dim)
        alias = f"{dim}_num"
        qs = self._annotate_dim(qs, alias, slug)
        return qs.filter(**{f"{alias}__gte": value})

    def max_filter(self, qs, name, value):
        dim = name.removesuffix("_max")
        slug = get_attr_slug(dim)
        alias = f"{dim}_num"
        qs = self._annotate_dim(qs, alias, slug)
        return qs.filter(**{f"{alias}__lte": value})

    # ======== Meta ========
    class Meta:
        model = Product
        # остальные поля (категория и т. д.) можно указывать здесь
        fields = {
            "category__slug": ["exact"],
        }