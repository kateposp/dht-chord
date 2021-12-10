from django.urls import path

from . import views

urlpatterns = [
    path('nodes', views.nodes, name='nodes'),
    path('nodes/<str:key>', views.node_data, name='gdata'),
    path('nodes/<str:key>/<str:value>', views.node_pdata, name='pdata'),
]