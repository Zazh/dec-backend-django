from rest_framework import serializers
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from .models import (
    Product, ProductCategory,
    ProductImage, ProductAttributeValue
)

class ProductImageSerializer(serializers.ModelSerializer):
    default_url = serializers.SerializerMethodField()
    preview_url = serializers.SerializerMethodField()

    class Meta:
        model = ProductImage
        fields = ["id", "default_url", "preview_url", "is_main"]

    def _abs(self, url):
        req = self.context.get("request")
        return req.build_absolute_uri(url) if req else url

    def get_default_url(self, obj):
        return self._abs(obj.thumb("default"))

    def get_preview_url(self, obj):
        return self._abs(obj.thumb("preview"))

class ProductAttributeValueSerializer(serializers.ModelSerializer):
    # ← название атрибута одной строкой
    attribute = serializers.CharField(source="attribute.name", read_only=True)
    value     = serializers.SerializerMethodField()

    class Meta:
        model  = ProductAttributeValue
        fields = ("attribute", "value")

    def get_value(self, obj):
        vt = obj.attribute.value_type
        v  = (obj.value or "").strip()

        if vt == "int":
            return int(v) if v.isdigit() else None

        if vt == "decimal":
            try:
                d = Decimal(v.replace(",", "."))
                # убираем экспоненту
                d = d.quantize(Decimal('1.'), rounding=ROUND_HALF_UP) if d == d.to_integral() else d.normalize()
                return float(d) if d.as_tuple().exponent else int(d)
            except (InvalidOperation, ValueError):
                return v     # оставим как строку, если не парсится

        if vt == "bool":
            return v.lower() in ("да", "yes", "true", "1")

        return v            # тип str


class ProductSerializer(serializers.ModelSerializer):
    category = serializers.SerializerMethodField()
    images = ProductImageSerializer(many=True, read_only=True)
    attributes = ProductAttributeValueSerializer(source="attribute_values", many=True, read_only=True)

    class Meta:
        model = Product
        fields = ["id", "title", "sku", "slug", "description", "price", "category", "images", "attributes"]

    def get_category(self, obj):
        if obj.category:
            return {
                "id": obj.category.id,
                "title": obj.category.title,
                "slug": obj.category.slug,
            }
        return None

# products/serializers.py
class CategorySerializer(serializers.ModelSerializer):
    image_default = serializers.SerializerMethodField()
    image_preview = serializers.SerializerMethodField()

    class Meta:
        model  = ProductCategory
        fields = ["id", "title", "slug", "parent_id",
                  "image_default", "image_preview"]

    def _abs(self, url):
        req = self.context.get("request")
        return req.build_absolute_uri(url) if req else url

    def get_image_default(self, obj):
        return self._abs(obj.thumb("default")) if obj.image else None

    def get_image_preview(self, obj):
        return self._abs(obj.thumb("preview")) if obj.image else None
