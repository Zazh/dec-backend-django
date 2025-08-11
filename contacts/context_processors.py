from .models import CompanyContact

def company_contacts(request):
    contact = CompanyContact.objects.prefetch_related('phones').first()
    return {
        'footer_contact': contact,
    }

def default_seo(request):
    return {
        'meta_description': 'Магазин декора и молдингов в Казахстане. Доставка по стране. Каталог товаров и выгодные предложения.',
        'meta_keywords': 'молдинги, декор, магазин, купить, Казахстан, интерьер, декоративные элементы',
        'og_title': 'Decorkz.kz — магазин молдингов и декора для интерьера',
        'og_description': 'Магазин декора и молдингов в Казахстане. Выгодные предложения и быстрая доставка.',
        'og_image': '/static/images/og-image.png',
    }