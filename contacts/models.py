# models.py
from django.db import models

class CompanyContact(models.Model):
    title = models.CharField("Название компании", max_length=255)

    def __str__(self):
        return self.title

class Phone(models.Model):
    contact = models.ForeignKey(CompanyContact, related_name='phones', on_delete=models.CASCADE)
    name = models.CharField("Название телефона", max_length=255)
    number = models.CharField("Номер телефона", max_length=50)

class Email(models.Model):
    contact = models.ForeignKey(CompanyContact, related_name='emails', on_delete=models.CASCADE)
    email = models.EmailField("Электронная почта")

class Social(models.Model):
    contact = models.ForeignKey(CompanyContact, related_name='socials', on_delete=models.CASCADE)
    svg_icon = models.TextField("SVG иконка")
    name = models.CharField("Название соцсети", max_length=255)
    url = models.URLField("Ссылка на соцсеть")

class Address(models.Model):
    contact = models.ForeignKey(CompanyContact, related_name='addresses', on_delete=models.CASCADE)
    address = models.CharField("Адрес", max_length=255)
    map_link = models.URLField("Ссылка на карту")

class PointOfSale(models.Model):
    title = models.CharField("Название точки", max_length=255)
    is_official = models.BooleanField("Фирменный салон", default=True)
    phones = models.TextField("Телефоны", help_text="Разделять переносом строки")
    schedule = models.TextField("График работы", help_text="Разделять переносом строки")
    address = models.CharField("Адрес", max_length=255)
    city = models.CharField("Город", max_length=100, default="Астана", blank=True)  # <--- НОВОЕ ПОЛЕ
    map_link = models.URLField("Ссылка на карту")