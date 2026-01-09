# -*- coding: utf-8 -*-
from __future__ import annotations
from django.http import Http404
from django.shortcuts import render
from .services.sqlite_service import get_movie_by_id
import math
from django.http import Http404, HttpRequest, HttpResponse


from .services.sqlite_service import (
    all_table_counts,
    list_genres,
    list_movies,
    search_all,
    stats_data,
)
from .services.mongo_service import (
    all_collection_counts,
    get_movie_complete,
    movie_ids_if_small,
)


def home(request: HttpRequest) -> HttpResponse:
    from .services.sqlite_service import list_movies

    # Top films (par note)
    top_movies, _ = list_movies(
        page=1, per_page=10, sort="rating_desc", rating_min=7.5
    )

    # Films récents (par année décroissante)
    recent_movies, _ = list_movies(
        page=1, per_page=15, sort="year_desc", year_min=2010
    )

    context = {
        "top_movies": top_movies,
        "recent_movies": recent_movies,
    }
    return render(request, "movies/home.html", context)



def movies_list(request: HttpRequest) -> HttpResponse:
    page = int(request.GET.get("page", "1") or 1)
    genre = request.GET.get("genre") or None
    sort = request.GET.get("sort") or "title_asc"

    def _to_int(v):
        try:
            return int(v) if v not in (None, "", "None") else None
        except ValueError:
            return None

    def _to_float(v):
        try:
            return float(v) if v not in (None, "", "None") else None
        except ValueError:
            return None

    year_min = _to_int(request.GET.get("year_min"))
    year_max = _to_int(request.GET.get("year_max"))
    rating_min = _to_float(request.GET.get("rating_min"))

    per_page = 20

    # IMPORTANT: si movies_complete est petit (ex: 200), on filtre la liste SQLite dessus
    only_ids = movie_ids_if_small(max_docs=5000)

    movies, total = list_movies(
        page=page,
        per_page=per_page,
        genre=genre,
        year_min=year_min,
        year_max=year_max,
        rating_min=rating_min,
        sort=sort,
        only_ids=only_ids,
    )

    nb_pages = max(1, math.ceil(total / per_page)) if total else 1

    context = {
        "movies": movies,
        "total": total,
        "page": page,
        "nb_pages": nb_pages,
        "genres": list_genres(),
        "filters": {
            "genre": genre or "",
            "year_min": "" if year_min is None else year_min,
            "year_max": "" if year_max is None else year_max,
            "rating_min": "" if rating_min is None else rating_min,
            "sort": sort,
        },
    }
    return render(request, "movies/movies_list.html", context)

def movie_detail(request, movie_id):
    movie = get_movie_by_id(movie_id)

    if not movie:
        raise Http404("Film introuvable")

    return render(request, "movies/movie_detail.html", {"movie": movie})

def search_view(request: HttpRequest) -> HttpResponse:
    q = request.GET.get("q", "") or ""
    movies, people = search_all(q)
    return render(request, "movies/search.html", {"q": q, "movies": movies, "people": people})


def stats_view(request: HttpRequest) -> HttpResponse:
    data = stats_data()

    # Adaptation des clés pour le template
    context = {
        "genres": [(r["genre"], r["n"]) for r in data.get("by_genre", [])],
        "decades": [(r["decade"], r["n"]) for r in data.get("by_decade", [])],
        "ratings_hist": data.get("ratings_hist", []),
        "top_actors": data.get("top_actors", []),
        "total_movies": sum(r["n"] for r in data.get("by_genre", [])),
        "total_people": len(data.get("top_actors", [])),
        "total_titles": sum(r["n"] for r in data.get("by_decade", [])),
    }
    return render(request, "movies/stats.html", context)
