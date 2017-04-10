from django.conf.urls import url, include

from . import views
from django.views.generic.base import RedirectView
from django.urls.base import reverse_lazy


urlpatterns = [
    url(r'^$', RedirectView.as_view(url=reverse_lazy('loadproc_list'), permanent=False)),
    url(r'^loadproc/$', views.LoadProcList.as_view(), name='loadproc_list'),
    url(r'^loadproc/create$', views.LoadProcCreate.as_view(), name='loadproc_create'),
    url(r'^loadproc/update/(?P<pk>\S+)$', views.LoadProcUpdate.as_view(), name='loadproc_update'),
    url(r'^loadproc/delete/(?P<pk>\S+)$', views.LoadProcDelete.as_view(), name='loadproc_delete'),
    url(r'^loadproc/upload/(?P<pk>\S+)$', views.UploadLoadProcByID, name='loadproc_upload'),
#     url(r'^loadlineproc/$', views.LoadLineProcList.as_view(), name='loadlineproc_list'),
    url(r'^loadlineproc/create/(?P<fk>\S+)$', views.LoadLineProcCreate.as_view(), name='loadlineproc_create'),
    url(r'^loadlineproc/update/(?P<pk>\S+)$', views.LoadLineProcUpdate.as_view(), name='loadlineproc_update'),
    url(r'^loadlineproc/delete/(?P<pk>\S+)$', views.LoadLineProcDelete.as_view(), name='loadlineproc_delete'),
    url(r'^loadproc_date/$', views.LoadProcByDate, name='loadproc_date'),
]