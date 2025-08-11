from django.contrib import admin
from .models import Post, ContentBlock

class ContentBlockInline(admin.TabularInline):
    model = ContentBlock
    extra = 1

class PostAdmin(admin.ModelAdmin):
    list_display = ('title', 'publish_date')
    inlines = [ContentBlockInline]

admin.site.register(Post, PostAdmin)
