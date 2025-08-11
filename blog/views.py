from django.shortcuts import render, get_object_or_404
from .models import Post

from products.models import ProductCategory, Product

def post_list(request):
    posts = Post.objects.order_by('-publish_date')
    return render(request, 'blog/post_list.html', {
        'posts': posts,
        "og_title": "Новости компании — Decorkz.kz",
        "meta_description": "Новости компании продавца лепнины, плинтусов, молдингов в Астане, Алматы, Казахстане о компании для партнеров и магазинов",
        "meta_keywords": "о компании Декор кз, о нас, партнеры",
        "og_description": "Новости компании — Decorkz.kz",
        "og_image": request.build_absolute_uri("/static/static/img/catalog-og.jpg"),
    })


def post_detail(request, slug):
    post = get_object_or_404(Post, slug=slug)

    # SEO-логика по шаблону:
    seo_title = f"{post.title} — Блог Decorkz.kz"
    seo_description = post.description[
                      :150] if post.description else f"Читайте статью «{post.title}» в блоге Decorkz.kz."
    og_image = request.build_absolute_uri(post.image.url) if post.image else request.build_absolute_uri(
        "/static/static/img/blog-og.jpg")
    og_description = seo_description
    og_title = seo_title
    meta_keywords = "блог, декор, новости, молдинги, лепнина, Казахстан"

    return render(request, 'blog/post_detail.html', {
        'post': post,
        "og_title": og_title,
        "meta_description": seo_description,
        "meta_keywords": meta_keywords,
        "og_description": og_description,
        "og_image": og_image,
    })


def home(request):
    categories = ProductCategory.objects.filter(parent__isnull=True).order_by("title")
    posts = Post.objects.order_by('-publish_date')[:4]
    latest = posts[0] if posts else None
    carousel_posts = posts[1:] if posts.count() > 1 else []
    return render(request, 'home.html', {
        'latest': latest,
        'carousel_posts': carousel_posts,
        "categories": categories,
    })