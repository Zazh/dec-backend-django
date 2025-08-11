# views.py
from django.shortcuts import render, get_object_or_404
from .models import CompanyContact, PointOfSale

def contacts_view(request):
    contacts = CompanyContact.objects.prefetch_related('phones', 'emails', 'socials', 'addresses').first()
    return render(request, 'contacts.html', {
        'contacts': contacts,
        "og_title": "Контакты ТОО Decorkz — Decorkz.kz",
        "meta_description": "Контакты компании ТОО Decorkz — Decorkz.kz, связаться с Decorkz в Астане, Алматы, Казахстане.",
        "meta_keywords": "каталог, декор, молдинги, плинтусы, рейки, интерьер, Казахстан, контакты, Decorkz, Decorkz.kz",
        "og_description": "Контакты ТОО Decorkz - decorkz.kz.",
        "og_image": request.build_absolute_uri("/static/static/img/catalog-og.jpg"),
    })

def points_of_sale_view(request):
    points = PointOfSale.objects.all()

    city = request.GET.get('city', '')
    point_type = request.GET.get('point_type', '')

    if city:
        points = points.filter(city=city)

    if point_type == 'official':
        points = points.filter(is_official=True)
    elif point_type == 'partner':
        points = points.filter(is_official=False)

    # Для фильтров
    cities = PointOfSale.objects.values_list('city', flat=True).distinct()

    return render(request, 'points_of_sale.html', {
        'points': points,
        'cities': cities,
        'selected_city': city,
        'selected_point_type': point_type,
        "og_title": "Точки продаж ТОО Decorkz по всему миру и в Казахстане — Decorkz.kz",
        "meta_description": "Точки продаж товаров компании ТОО Decorkz — decorkz.kz, список магазинов.",
        "meta_keywords": "молдинги, плинтусы, купить лепнину, Казахстан молдинги, Астана лепнина, Алматы лепнина, decorkz.kz",
        "og_description": "Точки продаж decorkz.kz",
        "og_image": request.build_absolute_uri("/static/static/img/catalog-og.jpg"),
    })
