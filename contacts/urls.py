# urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('contacts/', views.contacts_view, name='contacts'),
    path('points-of-sales/', views.points_of_sale_view, name='points_of_sales'),
]