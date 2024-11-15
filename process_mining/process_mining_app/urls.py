from django.urls import path
from .views import process_mining_view

urlpatterns = [
    path('', process_mining_view, name='process_mining'),
]
