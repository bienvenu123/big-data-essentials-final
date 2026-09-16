from pathlib import Path
import os

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR.parent / ".env")

SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "development-only-change-me")
DEBUG = os.getenv("DEBUG", "true").lower() == "true"
ALLOWED_HOSTS = [host.strip() for host in os.getenv("DJANGO_ALLOWED_HOSTS", "127.0.0.1,localhost").split(",")]
ROOT_URLCONF = "telecom_dashboard.urls"
INSTALLED_APPS = ["django.contrib.staticfiles", "rest_framework", "analytics"]
REST_FRAMEWORK = {"UNAUTHENTICATED_USER": None}
MIDDLEWARE = ["django.middleware.security.SecurityMiddleware", "django.middleware.common.CommonMiddleware"]
TEMPLATES = [{"BACKEND": "django.template.backends.django.DjangoTemplates", "DIRS": [BASE_DIR / "templates"], "APP_DIRS": True, "OPTIONS": {"context_processors": []}}]
DATABASES = {"default": {"ENGINE": "django.db.backends.mysql", "NAME": os.getenv("MYSQL_DATABASE", "essentials"), "USER": os.getenv("MYSQL_USER", "telecom_app"), "PASSWORD": os.getenv("MYSQL_PASSWORD"), "HOST": os.getenv("MYSQL_HOST", "localhost"), "PORT": os.getenv("MYSQL_PORT", "3306")}}
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
STATIC_URL = "static/"
