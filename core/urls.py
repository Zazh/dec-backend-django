from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.views.generic import TemplateView, RedirectView

from products.views import catalog, product_detail, catalog_root
from blog.views import home

urlpatterns = [
    path('', home, name='home'),
    path("product/<slug:slug>/", product_detail, name="product-detail"),
    path("catalog/<slug:category_slug>/", catalog, name="catalog-by-slug"),
    path("catalog/", catalog_root, name="catalog"),
    path('admin/', admin.site.urls),
    path('api/', include('products.urls', namespace='products')),
    path('blog/', include('blog.urls')),
    path('', include('contacts.urls')),
    path("robots.txt", TemplateView.as_view(template_name="robots.txt", content_type="text/plain")),

    # Price list
    path('price/', RedirectView.as_view(url='/static/price/index.html', permanent=False), name='price_list'),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)