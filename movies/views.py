from django.shortcuts import render

# Create your views here.
from django.http import JsonResponse
from .services.sqlite_service import all_table_counts
from .services.mongo_service import all_collection_counts, hello_info
from django.http import Http404
from .services.sqlite_service import (
    list_movies, list_genres, search_all, stats_data
)
from .services.mongo_service import get_movie_complete

def stats_view(request):
    data = {
        "sqlite": {
            "path": str(request.build_absolute_uri()),
            "tables_counts_sample": all_table_counts(limit=20),
        },
        "mongo": {
            "db": "from settings.MONGO_DB_NAME",
            "collections_counts_sample": all_collection_counts(limit=20),
            "hello": {
                "me": hello_info().get("me"),
                "primary": hello_info().get("primary"),
                "isWritablePrimary": hello_info().get("isWritablePrimary"),
                "setName": hello_info().get("setName"),
            }
        }
    }
    return JsonResponse(data, json_dumps_params={"indent": 2})

def home(request):
    # si tu l’as déjà, garde ta version
    return render(request, "movies/home.html")

def movies_list(request):
    page = int(request.GET.get("page", 1))
    page_size = 20

    genre = request.GET.get("genre") or None
    year_min = request.GET.get("year_min") or None
    year_max = request.GET.get("year_max") or None
    rating_min = request.GET.get("rating_min") or None
    sort = request.GET.get("sort", "title_asc")

    total, movies = list_movies(page, page_size, genre, year_min, year_max, rating_min, sort)
    genres = list_genres()

    nb_pages = max(1, (total + page_size - 1) // page_size)

    ctx = {
        "movies": movies,
        "genres": genres,
        "page": page,
        "nb_pages": nb_pages,
        "total": total,
        "filters": {"genre": genre, "year_min": year_min, "year_max": year_max, "rating_min": rating_min, "sort": sort}
    }
    return render(request, "movies/movies_list.html", ctx)

def movie_detail(request, movie_id: str):
    doc = get_movie_complete(movie_id)
    if not doc:
        raise Http404("Film introuvable (movies_complete)")
    return render(request, "movies/movie_detail.html", {"movie": doc})

def search(request):
    q = (request.GET.get("q") or "").strip()
    movies, people = ([], [])
    if q:
        movies, people = search_all(q)
    return render(request, "movies/search.html", {"q": q, "movies": movies, "people": people})

def stats(request):
    data = stats_data()
    return render(request, "movies/stats.html", data)
