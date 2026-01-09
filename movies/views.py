# movies/views.py
"""
Module Vue pour l'application CinéExplorer.
Gère le rendu des templates et l'intégration des données provenant des services SQLite et MongoDB.

Fonctionnalités:
- Listage et filtrage des films (SQLite)
- Détail des films avec enrichissement MongoDB
- Recherche multi-base
- Statistiques avec visualisations Chart.js
"""
from __future__ import annotations

from django.http import Http404
from django.shortcuts import render

from .services.sqlite_service import (
    all_table_counts,
    get_movie_by_id,
    list_genres,
    list_movies,
    list_top_movies,
    list_recent_movies,
    search_all,
    stats_data,
)
from .services.mongo_service import get_movie_complete


def home(request):
    # Home = best rated movies (page 1)
    movies, total = list_movies(page=1, page_size=24, order="rating")
    return render(request, "movies/home.html", {
        "movies": movies,
        "total": total,
        "top_movies": list_top_movies(12),
        "recent_movies": list_recent_movies(12)
    })


def movies_list(request):
    page = int(request.GET.get("page", "1") or 1)
    order = request.GET.get("order", "rating") or "rating"
    year_min = request.GET.get("year_min")
    year_max = request.GET.get("year_max")
    rating_min = request.GET.get("rating_min")
    
    # Convert to proper types, filtering out "None" strings and empty values
    year_min = int(year_min) if year_min and year_min != "None" else None
    year_max = int(year_max) if year_max and year_max != "None" else None
    rating_min = float(rating_min) if rating_min and rating_min != "None" else None
    
    movies, total = list_movies(
        page=page,
        page_size=24,
        order=order,
        year_min=year_min,
        year_max=year_max,
        rating_min=rating_min
    )
    return render(
        request,
        "movies/movies_list.html",
        {
            "movies": movies,
            "total": total,
            "page": page,
            "order": order,
            "year_min": year_min,
            "year_max": year_max,
            "rating_min": rating_min
        },
    )


def movie_detail(request, movie_id: str):
    """
    Détail complet d'un film avec intégration multi-base.
    
    Stratégie multi-base:
    1. Récupération des données relationnelles depuis SQLite (base primaire)
    2. Enrichissement avec données documentaires depuis MongoDB (si disponible)
    3. Fallback gracieux si MongoDB est indisponible
    
    Paramètres:
        request: HttpRequest
        movie_id (str): Identifiant IMDB du film (ex: 'tt0111161')
    
    Retour:
        HttpResponse: Template détail avec données fusionnées
    
    Lève:
        Http404: Si le film n'existe dans aucune base de données
    """
    # Récupération des données structurées depuis SQLite
    # (réalisateurs, scénaristes, cast, genres, ratings)
    movie = get_movie_by_id(movie_id)
    if not movie:
        raise Http404("Film introuvable")
    
    # Tentative d'enrichissement avec données MongoDB (collection pré-agrégée)
    try:
        mongo_movie = get_movie_complete(movie_id)
        if mongo_movie:
            # Fusion des données - MongoDB complète SQLite sans l'écraser
            movie["mongo_data"] = mongo_movie
    except Exception:
        # Dégradation gracieuse - utilise données SQLite seules en cas d'erreur
        pass
    
    return render(request, "movies/movie_detail.html", {"movie": movie})


def search_view(request):
    q = (request.GET.get("q") or "").strip()
    results = search_all(q) if q else {"movies": [], "persons": []}
    return render(
        request,
        "movies/search.html",
        {"q": q, "movies": results["movies"], "persons": results["persons"]},
    )


def stats_view(request):
    data = stats_data()
    # If your template wants these:
    return render(
        request,
        "movies/stats.html",
        {
            "stats": data,
            "tables": all_table_counts(),
            "genres": list_genres(),
        },
    )
