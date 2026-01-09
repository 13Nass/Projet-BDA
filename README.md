# CinéExplorer - Système Multi-Base de Données pour Analyse de Films IMDB

## Description

CinéExplorer est une application web full-stack développée avec **Django**, **Tailwind CSS** et **Chart.js** pour l'exploration et l'analyse du dataset IMDB. 

L'application implémente une **stratégie d'intégration multi-bases** :
- **SQLite** : Base primaire pour requêtes OLTP (listage, filtrage, recherche)
- **MongoDB (Replica Set)** : Base documentaire pour données pré-agrégées et enrichissement

### Fonctionnalités Principales
- Listage paginé des films avec filtres multi-critères (année, note)
- Détail complet film avec intégration MongoDB
- Recherche full-text multi-entité (films + personnes)
- Tableau de bord statistique avec graphiques Chart.js
- Système de notifications Toast (feedback utilisateur)
- Design responsive avec Tailwind CSS

---

## Installation et Démarrage

### Prérequis
- Python 3.14+
- MongoDB Community Edition
- pip (gestionnaire de paquets Python)

### Étape 1 : Préparation de l'Environnement

```bash
# Accéder à la racine du projet
cd "Racine du projet"

# Créer un environnement virtuel Python (optionnel mais recommandé)
python -m venv venv
venv\Scripts\activate

# Installer les dépendances Django
pip install django pymongo
```

### Étape 2 : Lancer MongoDB en Replica Set

MongoDB doit fonctionner en **Replica Set** pour cette application (requis pour transactions distribuées).

Ouvrir **3 terminaux distincts** et exécuter les commandes suivantes dans la racine du projet :

#### Terminal 1 - Instance MongoDB Port 27017
```bash
mongod --replSet rs0 --port 27017 --dbpath .\data\mongo\db-1 --bind_ip 127.0.0.1 --logpath .\data\mongo\logs\mongod-27017.log --logappend
```

#### Terminal 2 - Instance MongoDB Port 27018
```bash
mongod --replSet rs0 --port 27018 --dbpath .\data\mongo\db-2 --bind_ip 127.0.0.1 --logpath .\data\mongo\logs\mongod-27018.log --logappend
```

#### Terminal 3 - Instance MongoDB Port 27019
```bash
mongod --replSet rs0 --port 27019 --dbpath .\data\mongo\db-3 --bind_ip 127.0.0.1 --logpath .\data\mongo\logs\mongod-27019.log --logappend
```

**Initialiser le Replica Set** (une seule fois, dans un terminal MongoDB) :
```bash
# Se connecter à une instance MongoDB
mongosh --port 27017

# Initialiser le replica set
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "127.0.0.1:27017" },
    { _id: 1, host: "127.0.0.1:27018" },
    { _id: 2, host: "127.0.0.1:27019" }
  ]
})

# Vérifier l'état
rs.status()
```

### Étape 3 : Lancer le Serveur Django

Dans un **4ème terminal**, à la racine du projet :

```bash
# Appliquer les migrations Django (première exécution uniquement)
python manage.py migrate

# Lancer le serveur de développement
python manage.py runserver
```

Le serveur démarre sur `http://127.0.0.1:8000`

---

## Structure du Projet

```
C:\Users\bendr\OneDrive\Documents\Polytech\S7\BDA\
├── config/                     # Configuration Django
│   ├── settings.py            # Paramètres (DB, apps, middleware)
│   ├── urls.py               # Routes principales
│   ├── wsgi.py               # Interface serveur
│   └── asgi.py               # Interface async
│
├── movies/                     # Application principale
│   ├── models.py             # Modèles ORM (Django)
│   ├── views.py              # Vues (contrôleurs)
│   ├── urls.py               # Routes de l'app
│   ├── services/
│   │   ├── sqlite_service.py  # Requêtes SQLite (couche données)
│   │   └── mongo_service.py   # Requêtes MongoDB (enrichissement)
│   └── templates/
│       └── movies/           # Templates HTML
│           ├── home.html
│           ├── movies_list.html
│           ├── movie_detail.html
│           ├── search.html
│           ├── stats.html
│           └── base.html     # Template parent (navbar, footer)
│
├── data/
│   ├── imdb.db               # Base SQLite primaire
│   ├── csv/                  # Exports CSV source
│   └── mongo/                # Données MongoDB
│       ├── db-1/             # Instance replica 1
│       ├── db-2/             # Instance replica 2
│       └── db-3/             # Instance replica 3
│
├── manage.py                 # Utilitaire Django
├── db.sqlite3                # Base Django auth/sessions
└── README.md                 # Ce fichier
```

---

## 🗄️ Architecture Multi-Bases

### SQLite (Base Primaire - OLTP)
**Utilisé pour :**
- Requêtes relationnelles rapides (listage, filtrage)
- Recherche full-text LIKE sur films/personnes
- Statistiques agrégées (COUNT, AVG)

**Tables principales :**
- `movies` - Informations films (titre, année, type)
- `ratings` - Notes IMDB (rating, num_votes)
- `genres` - Genres par film
- `persons` - Acteurs, réalisateurs, scénaristes
- `directors`, `writers`, `principals` - Relations film→personne

**Performance :**
- Indexation sur `(title_type, start_year, average_rating)`
- Queries : O(log n) avec pagination LIMIT/OFFSET

### MongoDB (Base Documentaire - Enrichissement)
**Utilisé pour :**
- Données pré-agrégées (collection `movies_complete`)
- Enrichissement détail film (métadonnées supplémentaires)
- Fallback gracieux si données partielles

**Collections :**
- `movies_complete` - Documents films aggrégés

**Stratégie Intégration :**
```
Vue movie_detail():
  1. Récupère données SQLite (acteurs, réalisateurs, genres)
  2. Fusionne avec MongoDB si disponible
  3. Fallback à SQLite seul en cas d'erreur
```

---

## 🎨 Fonctionnalités Implémentées

### 1. **Listage et Filtrage Films** (`/movies/`)
- Pagination 24 films/page
- Filtres dynamiques :
  - Année production (min/max)
  - Note IMDB minimum
  - Tri : rating, votes, année, titre
- Requête SQL optimisée avec GROUP BY/JOIN

### 2. **Détail Film** (`/movies/<movie_id>/`)
- Données SQLite : casting, réalisateurs, genres, ratings
- Enrichissement MongoDB : métadonnées pré-agrégées
- Gestion d'erreur gracieuse (fallback SQLite)

### 3. **Recherche Multi-Base** (`/search/?q=...`)
- Recherche films : LIKE sur primary_title, original_title
- Recherche personnes : LIKE sur name
- Résultats limités 20 + 20 pour UX performante

### 4. **Statistiques & Visualisations** (`/stats/`)
- Graphiques Chart.js interactifs :
  - **Doughnut** : Distribution genres
  - **Bar Chart** : Statistiques globales (films, personnes, ratings)
- Statistiques agrégées : AVG(rating), AVG(votes), COUNT(*)

### 5. **Feedback Utilisateur**
- **Toast Notifications** : Messages animés (success/error/info)
- **Spinners** : Feedback visuel lors filtrage
- **Animations CSS** : Slide-in/Slide-out fluides

---

## ⚙️ Configuration Django

### Settings (`config/settings.py`)

```python
# Chemins des bases de données
IMDB_SQLITE_PATH = 'C:/Users/bendr/OneDrive/Documents/Polytech/S7/BDA/data/imdb.db'

# MongoDB
MONGO_URI = 'mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?replicaSet=rs0'
MONGO_DB_NAME = 'cineexplorer_flat'

# Apps installées
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'movies',  # Application principale
]
```

---

## 📊 Exemples de Requêtes

### Listage films filtrés (SQLite)
```python
# Vue movies_list() avec filtres année et note
movies, total = list_movies(
    page=2,
    page_size=24,
    order="rating",
    year_min=2015,
    year_max=2023,
    rating_min=7.5
)
```

### Détail film enrichi (SQLite + MongoDB)
```python
# Vue movie_detail() avec fusion multi-base
movie = get_movie_by_id("tt0111161")  # SQLite
mongo_data = get_movie_complete("tt0111161")  # MongoDB
movie["mongo_data"] = mongo_data  # Fusion
```

### Statistiques (SQLite)
```python
stats = stats_data()
# {
#   "total_movies": 386_000,
#   "total_persons": 10_500_000,
#   "avg_rating": 6.8,
#   "avg_votes": 125_000
# }
```

---

## 🔧 Commandes Utiles

```bash
# Django
python manage.py migrate              # Appliquer migrations
python manage.py createsuperuser      # Créer admin
python manage.py runserver            # Démarrer serveur (port 8000)

# MongoDB (mongosh)
mongosh --port 27017                  # Se connecter instance 1
rs.status()                           # Vérifier replica set
db.movies_complete.count()            # Compter documents

# Développement
pip freeze > requirements.txt         # Exporter dépendances
python -m venv venv                   # Créer env virtuel
```

---

## 📈 Performance

### Optimisations Implémentées
1. **Pagination** : LIMIT/OFFSET 24 items/page
2. **Indexation SQLite** : Sur colonnes filtrage (year, rating)
3. **GROUP_CONCAT** : Agrégation genres en une seule requête
4. **LEFT JOIN** : Évite doublons avec ratings/genres
5. **Caching MongoDB** : Replica set pour haute disponibilité

### Complexité Requêtes
| Opération | Complexité | Notes |
|-----------|-----------|-------|
| Listage films | O(log n) | Avec index sur (type, year, rating) |
| Détail film | O(m) | m = nombre cast/réalisateurs |
| Recherche LIKE | O(n) | Full-table scan (acceptable < 1M films) |
| Statistiques | O(n) | Agrégation complète table |

---

## 🎯 Architecture UX/UI

### Stack Frontend
- **Framework CSS** : Tailwind CSS (CDN)
- **Graphiques** : Chart.js
- **Animations** : CSS keyframes (spinners, toasts)
- **Design** : Responsive mobile-first avec dark theme galaxy

### Responsive Breakpoints
```css
xs: 0px       (mobile)
sm: 640px     (tablet)
md: 768px     (small laptop)
lg: 1024px    (desktop)
xl: 1280px    (large desktop)
```

---

## 🐛 Dépannage

### MongoDB ne se connecte pas
```bash
# Vérifier que mongod s'exécute sur les 3 ports
# Vérifier que le replica set est initialisé
mongosh --port 27017
> rs.status()
```

### SQLite base vide
```bash
# Vérifier le chemin IMDB_SQLITE_PATH dans settings.py
# Importer les données CSV si nécessaire
python manage.py import_data
```

### Port 8000 déjà utilisé
```bash
# Utiliser port différent
python manage.py runserver 8001
```

---

## 📚 Ressources

- [Django Documentation](https://docs.djangoproject.com/)
- [MongoDB Replica Sets](https://docs.mongodb.com/manual/replication/)
- [Chart.js](https://www.chartjs.org/)
- [Tailwind CSS](https://tailwindcss.com/)

---

## Checklist Complet Démarrage

- [ ] Python 3.14+ installé
- [ ] MongoDB Community Edition installé
- [ ] Environnement virtuel créé et activé
- [ ] Dépendances installées (`pip install django pymongo`)
- [ ] 3 instances mongod lancées (ports 27017, 27018, 27019)
- [ ] Replica set initialisé (`rs.initiate()`)
- [ ] Serveur Django lancé (`python manage.py runserver`)
- [ ] Accès à `http://127.0.0.1:8000` OK
- [ ] Page `/stats/` affiche graphiques Chart.js
- [ ] Filtrage films fonctionne (`/movies/?year_min=2020`)

---

## 📝 Licence et Auteur

Projet développé pour le cours BDA (Polytech S7)  
Dataset source : [IMDb](https://www.imdb.com/)

---

**Version** : 1.0  
**Dernière mise à jour** : 9 janvier 2026