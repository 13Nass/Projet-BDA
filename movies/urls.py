# -*- coding: utf-8 -*-
"""
Created on Tue Dec 30 16:24:46 2025

@author: bendr
"""

from django.urls import path
from .views import stats_view

urlpatterns = [
    path("stats/", stats_view, name="stats"),
]
