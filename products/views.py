# views.py
from rest_framework import filters, viewsets
from django.shortcuts import render, get_object_or_404
from django.db.models import Q, OuterRef
import django_filters as df


from .filters import ProductFilter
from .filters_config import FILTER_CONFIG

from django.core.paginator import Paginator

from .services import StandardResultsSetPagination
from .models import Product, ProductCategory, ProductAttributeValue, Attribute, AttributeGroup
from .serializers import ProductSerializer, CategorySerializer
from django_filters.rest_framework import DjangoFilterBackend, BooleanFilter

class ProductViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = (
        Product.objects
        .select_related("category")
        .prefetch_related(
            "images",                     # галерея
            "attribute_values__attribute" # ← правильный путь
        )
    )
    lookup_field = "slug"
    serializer_class = ProductSerializer
    filter_backends   = [
        DjangoFilterBackend,
        filters.SearchFilter,
        filters.OrderingFilter,
    ]
    filterset_class  = ProductFilter
    # ➊ Фильтры django-filter
    filterset_fields = {
        "category__slug": ["exact"],
    }

    # ➋ Полнотекстовый поиск
    search_fields = ["title", "description", "sku"]

    # ➌ Сортировка
    ordering_fields = ["price", "title"]

    # ➍ Стандартная пагинация (если нужна)
    pagination_class = StandardResultsSetPagination


class CategoryFilter(df.FilterSet):
    parent__isnull = BooleanFilter(field_name='parent', lookup_expr='isnull')

    class Meta:
        model = ProductCategory
        fields = ['parent', 'parent__isnull']

class CategoryViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = ProductCategory.objects.all()
    serializer_class = CategorySerializer
    filter_backends = [DjangoFilterBackend]
    filterset_class = CategoryFilter

# --- Получаем slug-и атрибутов ---
def get_attr_slug(attr_name):
    try:
        return Attribute.objects.only("slug").get(name__iexact=attr_name).slug
    except Attribute.DoesNotExist:
        return None

def apply_range_filters(queryset, field, values, attr_slug=None):
    """
    Фильтрация по диапазонам с поддержкой OR для чекбоксов.
    - field: 'price', 'length', 'width', 'height'
    - values: ['0-5000', '10000-', ...]
    - attr_slug: slug атрибута (для length/width/height)
    """
    if not values:
        return queryset

    q_filter = Q()
    for val in values:
        min_val, max_val = None, None
        parts = val.split('-')
        if len(parts) == 2:
            min_val = parts[0] if parts[0] else None
            max_val = parts[1] if parts[1] else None
        elif parts:
            min_val = parts[0]

        if field == 'price':
            cond = Q()
            if min_val:
                cond &= Q(price__gte=float(min_val))
            if max_val:
                cond &= Q(price__lte=float(max_val))
            q_filter |= cond
        else:
            subq = ProductAttributeValue.objects.filter(
                product=OuterRef('pk'),
                attribute__slug=attr_slug
            )
            if min_val:
                subq = subq.filter(value__gte=min_val)
            if max_val:
                subq = subq.filter(value__lte=max_val)
            q_filter |= Q(pk__in=subq.values('product_id'))

    return queryset.filter(q_filter)

def format_number(value):
    try:
        return ('{:.2f}'.format(float(value))).rstrip('0').rstrip('.')
    except (ValueError, TypeError):
        return str(value)


def catalog_root(request):
    categories = ProductCategory.objects.filter(parent__isnull=True).order_by("title")
    return render(request, "catalog_root.html", {
        "categories": categories,
        "og_title": "Каталог товаров — Decorkz.kz",
        "meta_description": "Каталог товаров: молдинги, плинтусы, рейки, декоративные элементы для интерьера. Прямые поставки. Лучшие цены в Казахстане.",
        "meta_keywords": "каталог, декор, молдинги, плинтусы, рейки, интерьер, Казахстан",
        "og_description": "Весь ассортимент молдингов и декоративных элементов для интерьера. Каталог товаров Decorkz.kz.",
        "og_image": request.build_absolute_uri("/static/static/img/catalog-og.jpg"),
    })


def catalog(request, category_slug=None):
    category = None
    categories = list(ProductCategory.objects.values("id", "title", "slug").order_by("title"))
    seo_title = "Каталог товаров decorkz.kz"
    seo_description = "decor.kz - производим декоративные решения, карнизы, плинтусы, рейки"
    allowed_orderings = ["title", "-title", "price", "-price"]
    filter_config = FILTER_CONFIG.get(category_slug, FILTER_CONFIG['default'])

    products = []
    attribute_values = {}
    vid_values = []
    page_obj = None

    price_values = request.GET.getlist('price') if category_slug else []
    length_values = request.GET.getlist('length') if category_slug else []
    width_values = request.GET.getlist('width') if category_slug else []
    height_values = request.GET.getlist('height') if category_slug else []
    vid_selected = request.GET.getlist('vid') if category_slug else []

    if category_slug:
        category = get_object_or_404(ProductCategory, slug=category_slug)
        # 1. Сначала ищем подкатегории
        subcategories = ProductCategory.objects.filter(parent=category)

        if category.image and category.thumb("preview"):
            og_image = request.build_absolute_uri(category.thumb("preview"))
        else:
            og_image = request.build_absolute_uri("/static/static/img/catalog-og.jpg")

        if subcategories.exists():
            # Если есть подкатегории — показываем их (без товаров)
            return render(request, "catalog_category.html", {
                "category": category,
                "subcategories": subcategories,
                "seo_title": f"{category.title} — Категория",
                "seo_description": f"Список подкатегорий {category.title}",
                "meta_description": f"Список подкатегорий {category.title}",
                "meta_keywords": f"{category.title}, каталог, подкатегории, декор, молдинги",
                "og_image": og_image,
                "og_title": f"{category.title} — Категория",
                "og_description": f"Список подкатегорий {category.title}",
            })

        # 2. Если подкатегорий нет — показываем товары
        children_ids = list(category.children.values_list('id', flat=True))
        category_ids = [category.id] + children_ids

        products_qs = Product.objects.filter(category__id__in=category_ids).prefetch_related(
            'images', 'attribute_values__attribute'
        )

        for attr_name in filter_config.get('attributes', []):
            values = ProductAttributeValue.objects.filter(
                attribute__name__iexact=attr_name,
                product__category=category
            ).values_list('value', flat=True).distinct()
            attribute_values[attr_name] = list(values)

        length_slug = get_attr_slug('длина')
        width_slug = get_attr_slug('ширина')
        height_slug = get_attr_slug('высота')

        products_qs = apply_range_filters(products_qs, 'price', price_values)
        products_qs = apply_range_filters(products_qs, 'length', length_values, length_slug)
        products_qs = apply_range_filters(products_qs, 'width', width_values, width_slug)
        products_qs = apply_range_filters(products_qs, 'height', height_values, height_slug)

        if 'вид' in filter_config.get('attributes', []) and vid_selected:
            products_qs = products_qs.filter(
                attribute_values__attribute__name__iexact='вид',
                attribute_values__value__in=vid_selected
            ).distinct()

        ordering = request.GET.get("ordering")
        if ordering in allowed_orderings:
            products_qs = products_qs.order_by(ordering)
        else:
            products_qs = products_qs.order_by('id')

        products = [
            {
                "id": p.id,
                "title": p.title,
                "slug": p.slug,
                "price": int(p.price),
                "image": p.images.first().thumb("preview") if p.images.exists() else "",
                "alt": f"{p.category.title} {p.title}",
                "title_attr": f"{p.category.title} {p.title}",
                "category_title": p.category.title,
                "size": " × ".join([
                    format_number(next((a.value for a in p.attribute_values.all() if a.attribute.name.lower() == 'высота'), '')),
                    format_number(next((a.value for a in p.attribute_values.all() if a.attribute.name.lower() == 'ширина'), '')),
                    format_number(next((a.value for a in p.attribute_values.all() if a.attribute.name.lower() == 'длина'), '')),
                ]),
                "type": next((a.value for a in p.attribute_values.all() if a.attribute.name.lower() == 'вид'), 'не указан'),
            }
            for p in products_qs
        ]

        seo_title = f"{category.title} — Купить по лучшей цене"
        seo_description = f"{category.title}: широкий ассортимент по доступным ценам."

        vid_values = attribute_values.get('вид', [])

        paginator = Paginator(products, 20)
        page_obj = paginator.get_page(request.GET.get('page', 1))

    context = {
        "categories": categories,
        "products": products,
        "current_category": category,
        "seo_title": seo_title,
        "seo_description": seo_description,
        "meta_description": seo_description,
        "meta_keywords": "каталог, декор, молдинги, купить, Казахстан",  # можно добавить или брать из категории
        "og_title": seo_title,
        "og_description": seo_description,
        "attribute_values": attribute_values,
        "filter_config": filter_config,
        "length_values": length_values,
        "width_values": width_values,
        "height_values": height_values,
        "price_values": price_values,
        "vid_selected": vid_selected,
        "vid_values": vid_values,
        "page_obj": page_obj,
        "podsvetka_values": attribute_values.get('подсветка', []),
    }
    return render(request, "catalog.html", context)

def build_size_string(sizes: dict) -> str:
    parts = []
    for key in ['height', 'width', 'length']:
        value = sizes.get(key)
        if value not in (None, '', 'None'):
            parts.append(str(value))
    if parts:
        return ' × '.join(parts) + ' мм'
    return ""


def product_detail(request, slug):
    product = get_object_or_404(Product, slug=slug)
    # Загружаем все связанные атрибуты одной строкой:
    attributes = product.attribute_values.select_related('attribute')

    base_group_name = AttributeGroup.objects.filter(
        template__is_base=True).first().title  # если есть только один базовый

    # атрибуты группы
    attribute_groups = product.get_attribute_groups()

    # Сортируем их по привычному порядку (чтобы всегда было В, Ш, Д, потом остальные):
    attrs_map = {a.attribute.name.lower(): a.value for a in attributes}

    # Формируем структуру для шаблона
    sizes = {
        'height': format_number(attrs_map.get('высота')),
        'width': format_number(attrs_map.get('ширина')),
        'length': format_number(attrs_map.get('длина')),
    }
    size_string = build_size_string(sizes)  # ← вызываем функцию
    vid = attrs_map.get('вид')
    podsvetka = attrs_map.get('подсветка')

    # Выводим остальные (не размеры, не вид, не подсветка) как "прочие характеристики":
    other_attributes = [
        {'name': a.attribute.name, 'value': a.value}
        for a in attributes
        if a.attribute.name.lower() not in ['высота', 'ширина', 'длина', 'вид', 'подсветка']
    ]

    seo_title = f"{product.title} — купить по лучшей цене"
    seo_description = (product.description or f"Купить {product.title} по доступной цене. Характеристики, фото, доставка.")

    def get_category_chain(category):
        chain = []
        while category:
            chain.append(category)
            category = category.parent
        return list(reversed(chain))

    category_chain = get_category_chain(product.category)

    if product.images.exists():
        main_image = product.images.filter(is_main=True).first() or product.images.first()
        og_image = request.build_absolute_uri(main_image.thumb("preview"))
    else:
        og_image = "/static/static/img/catalog-og.jpg"  # дефолтная картинка если фото нет


    context = {
        "product": product,
        "seo_title": seo_title,
        "seo_description": seo_description,
        "meta_description": seo_description,
        "meta_keywords": "каталог, декор, молдинги, купить, Казахстан",  # можно добавить или брать из категории
        "og_title": seo_title,
        "og_description": seo_description,
        "og_image": og_image,  # ← вот оно!
        "sizes": sizes,
        "size_string": size_string,  # ← кладём строку в контекст
        "vid": vid,
        "podsvetka": podsvetka,
        "other_attributes": other_attributes,
        "category_chain": category_chain,  # цепочка категорий для хлебных крошек
        "attribute_groups": attribute_groups,
        'base_group_name': base_group_name,

    }
    return render(request, "product.html", context)