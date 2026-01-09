# movies/views.py
from __future__ import annotations

from django.http import Http404
from django.shortcuts import render

from .services.sqlite_service import (
    all_table_counts,
    get_movie_by_id,
    list_genres,
    list_movies,
    search_all,
    stats_data,
)


def home(request):
    # Home = best rated movies (page 1)
    movies, total = list_movies(page=1, page_size=24, order="rating")
    return render(request, "movies/home.html", {"movies": movies, "total": total})


def movies_list(request):
    page = int(request.GET.get("page", "1") or 1)
    order = request.GET.get("order", "rating") or "rating"
    movies, total = list_movies(page=page, page_size=24, order=order)
    return render(
        request,
        "movies/movies_list.html",
        {"movies": movies, "total": total, "page": page, "order": order},
    )


def movie_detail(request, movie_id: str):
    movie = get_movie_by_id(movie_id)
    if not movie:
        raise Http404("Film introuvable")
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
