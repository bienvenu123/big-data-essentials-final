from django.urls import path
from analytics.views import ingest_event, overview

urlpatterns = [path("", overview, name="overview"), path("api/events/", ingest_event, name="ingest-event")]
