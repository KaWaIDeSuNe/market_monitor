"""
Django settings for traing_system_master project.

Generated by 'django-admin startproject' using Django 2.2.5.

For more information on this file, see
https://docs.djangoproject.com/en/2.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/2.2/ref/settings/
"""

import os
from kombu import Queue, Exchange


from collections import namedtuple
from enum import Enum

CELERY_TIMEZONE = 'Asia/Shanghai'  # 并没有北京时区，与下面TIME_ZONE应该一致

redis_host = os.environ.get('CELERY_REDIS_HOST', "192.168.10.79")
redis_pwd = os.environ.get('CELERY_REDIS_PASS', "shiye1805A")

# django celery config
CELERY_BROKER_URL = "redis://:" + redis_pwd + "@" + redis_host + ":8410/15"
CELERY_RESULT_BACKEND = "redis://:" + redis_pwd + "@" + redis_host + ":8410/14"
CELERY_RESULT_SERIALIZER = 'json'


# CELERY_QUEUES = (
#     Queue("default", Exchange("default"), routing_key = "default"),
#     Queue("for_task_A", Exchange("for_task_A"), routing_key = "for_task_A"),
#     Queue("for_task_B", Exchange("for_task_B"), routing_key = "for_task_B")
# )
# #路由
# # CELERY_ROUTES = {
# #     "controllers.annual_report.tasks.annual_report_master":{"queue": "for_task_A",  "routing_key": "for_task_A"},
# # }
#
# CELERY_ROUTES = (
#    [
#        ("controllers.annual_report.tasks.*", {"queue": "default"}), # 将tasks模块中的所有任务分配至队列 default
#    ],
# )

CELERYBEAT_SCHEDULER = 'djcelery.schedulers.DatabaseScheduler'  ###


# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/2.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'g&m0)hwdyvlfa$97behm2k25b7if32-xibv$evxdy1o5roy=xg'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = os.environ.get('DEBUG', True)

ALLOWED_HOSTS = ['*']

# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django_celery_results',
    'django_celery_beat',
    'controllers.market_temp',
    'controllers.up_down_dis',
    'controllers.turnover_forecast',
    'controllers.up_down_forecast',
]

CELERY_IMPORTS = (
    'controllers.market_temp.tasks',
    'controllers.up_down_dis.tasks',
    'controllers.turnover_forecast.tasks',
    'controllers.up_down_forecast.tasks',
)

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'market_monitor.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'market_monitor.wsgi.application'

# Database
# https://docs.djangoproject.com/en/2.2/ref/settings/#databases

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": os.environ.get("DB_NAME", "scheduler"),
        "USER": os.environ.get("DB_USER", "root"),
        "PASSWORD": os.environ.get("DB_PASSWORD", "Tom&Jerry"),
        "HOST": os.environ.get("DB_HOST", "172.20.117.55"),
        "PORT": os.environ.get("DB_PORT", "63306"),
    }
}

# Password validation
# https://docs.djangoproject.com/en/2.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
# https://docs.djangoproject.com/en/2.2/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'Asia/Shanghai'

USE_I18N = True

USE_L10N = True

USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/2.2/howto/static-files/

STATICFILES_DIRS = [
    os.path.join(BASE_DIR, 'polls', 'templates', 'static'),
]
STATIC_ROOT = os.path.abspath(os.path.join(BASE_DIR, 'static'))
STATIC_URL = '/static/'  #关键在这句。



