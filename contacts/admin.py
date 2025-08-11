# admin.py
from django.contrib import admin
from .models import CompanyContact, Phone, Email, Social, Address, PointOfSale

class PhoneInline(admin.TabularInline):
    model = Phone
    extra = 1

class EmailInline(admin.TabularInline):
    model = Email
    extra = 1

class SocialInline(admin.TabularInline):
    model = Social
    extra = 1

class AddressInline(admin.TabularInline):
    model = Address
    extra = 1

@admin.register(CompanyContact)
class CompanyContactAdmin(admin.ModelAdmin):
    inlines = [PhoneInline, EmailInline, SocialInline, AddressInline]

@admin.register(PointOfSale)
class PointOfSaleAdmin(admin.ModelAdmin):
    list_display = ['title', 'is_official']