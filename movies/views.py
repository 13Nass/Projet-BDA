from django.shortcuts import render

# Create your views here.
from django.http import JsonResponse
from .services.sqlite_service import all_table_counts
from .services.mongo_service import all_collection_counts, hello_info

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
