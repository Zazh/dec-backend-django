from django.db import models
from easy_thumbnails.fields import ThumbnailerImageField
from django.utils.text import slugify

class Post(models.Model):
    title = models.CharField(max_length=200)
    description = models.CharField(max_length=500)
    slug = models.SlugField(max_length=200, unique=False, blank=True, null=True)
    image = ThumbnailerImageField(upload_to='blog_images')
    publish_date = models.DateTimeField(auto_now_add=True)

    def save(self, *args, **kwargs):
        if not self.slug:
            self.slug = slugify(self.title)
        super().save(*args, **kwargs)

    def __str__(self):
        return self.title

class ContentBlock(models.Model):
    CONTENT_CHOICES = [
        ('p', 'Paragraph'),
        ('h2', 'Heading 2'),
        ('h3', 'Heading 3'),
        ('ul', 'Unordered List'),
        ('ol', 'Ordered List'),
    ]
    post = models.ForeignKey(Post, related_name='blocks', on_delete=models.CASCADE)
    content_type = models.CharField(max_length=2, choices=CONTENT_CHOICES)
    text = models.TextField()
    bold = models.BooleanField(default=False)
    class Meta:
        ordering = ['id']
