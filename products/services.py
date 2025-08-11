from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework.pagination import PageNumberPagination
from rest_framework import filters
from django_filters.rest_framework import DjangoFilterBackend

from .models import Product
from .serializers import ProductSerializer


class StandardResultsSetPagination(PageNumberPagination):
    """
    Универсальная пагинация:
    - page_size             — 20 объектов по умолчанию
    - page_size_query_param — ?page_size=30  (клиент может попросить меньше/больше)
    - max_page_size         — верхний предел в 100 строк
    """
    page_size = 20
    page_size_query_param = "page_size"
    max_page_size = 100


class ProductViewSet(ReadOnlyModelViewSet):
    """
    Только чтение (GET /list, GET /detail) — безопасно для PIM‑витрины.
    """
    queryset = (
        Product.objects
        .select_related("category")
        .prefetch_related("attributes__attribute")
        .all()
    )
    serializer_class = ProductSerializer          # <‑‑‑ ВОТ ЭТОГО НЕ ХВАТАЛО
    pagination_class = StandardResultsSetPagination

    # django-filter + DRF search/order backends
    filter_backends = [
        DjangoFilterBackend,
        filters.SearchFilter,
        filters.OrderingFilter,
    ]

    filterset_fields = {
        "category__slug": ["exact"],
        "price": ["gte", "lte"],
        "attributes__attribute__slug": ["exact"],
        "attributes__value": ["exact", "icontains"],
    }
    search_fields = ["sku", "name", "description"]
    ordering_fields = ["price", "name"]
