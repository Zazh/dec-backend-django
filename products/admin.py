from django.contrib import admin
from django import forms
from image_cropping import ImageCroppingMixin
from .models import (
    ProductCategory, Product, Attribute,
    ProductImage, ProductAttributeValue, AttributeTemplate, AttributeGroup
)

class ProductImageInline(ImageCroppingMixin, admin.TabularInline):
    model = ProductImage
    extra = 1
    fields = ("image", "cropping", "is_main", "thumbnail_preview")
    readonly_fields = ("thumbnail_preview",)

class ProductAttributeInline(admin.TabularInline):
    model = ProductAttributeValue
    extra = 0
    autocomplete_fields = ["attribute"]

@admin.register(ProductCategory)
class ProductCategoryAdmin(admin.ModelAdmin):
    list_display = ("title", "slug", "parent", "thumbnail_preview")
    search_fields = ("title",)

@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ("title", "category", "sku", "price")
    list_filter = ("category",)
    search_fields = ("title",)
    inlines = [ProductImageInline, ProductAttributeInline]

@admin.register(Attribute)
class AttributeAdmin(admin.ModelAdmin):
    search_fields = ("label",)

class ProductAttributeValueForm(forms.ModelForm):
    class Meta:
        model  = ProductAttributeValue
        fields = ("attribute", "value")

    def clean(self):
        # модель уже валидирует, но хотим показать ошибку до save()
        self.instance.attribute = self.cleaned_data.get("attribute")
        self.instance.value     = self.cleaned_data.get("value")
        self.instance.clean()      # вызовет ValidationError при неверном типе
        return self.cleaned_data

class ProductAttributeInline(admin.TabularInline):
    model  = ProductAttributeValue
    form   = ProductAttributeValueForm
    extra  = 0
    autocomplete_fields = ["attribute"]

@admin.register(AttributeGroup)
class AttributeGroupAdmin(admin.ModelAdmin):
    list_display = ('title', 'template')
    search_fields = ('title', 'template__title')
    autocomplete_fields = ['template', 'attributes']

@admin.register(AttributeTemplate)
class AttributeTemplateAdmin(admin.ModelAdmin):
    list_display = ('title', 'is_base')
    list_editable = ('is_base',)
    search_fields = ('title',)
    filter_horizontal = ('categories',)