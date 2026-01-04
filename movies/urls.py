# -*- coding: utf-8 -*-
"""
Created on Tue Dec 30 16:24:46 2025

@author: bendr
"""

from django.urls import path
from .views import stats_view
from . import views

urlpatterns = [
    path("", views.home, name="home"),
    path("movies/", views.movies_list, name="movies_list"),
    path("movies/<str:movie_id>/", views.movie_detail, name="movie_detail"),
    path("search/", views.search, name="search"),
    path("stats/", views.stats, name="stats"),
]