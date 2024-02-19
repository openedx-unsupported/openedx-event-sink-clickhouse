
"""
These settings are here to use during tests, because django requires them.

In a real-world use case, apps in this project are installed into other
Django applications, so these settings will not be used.
"""

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": "default.db",
        "USER": "",
        "PASSWORD": "",
        "HOST": "",
        "PORT": "",
    },
    "read_replica": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": "read_replica.db",
        "USER": "",
        "PASSWORD": "",
        "HOST": "",
        "PORT": "",
    },
}


INSTALLED_APPS = (
    "event_sink_clickhouse",
)

EVENT_SINK_CLICKHOUSE_MODEL_CONFIG = {
    "user_profile": {
        "module": "common.djangoapps.student.models",
        "model": "UserProfile",
    },
    "course_overviews": {
        "module": "openedx.core.djangoapps.content.course_overviews.models",
        "model": "CourseOverview",
    }
}

EVENT_SINK_CLICKHOUSE_COURSE_OVERVIEWS_ENABLED = True

FEATURES = {
    'CUSTOM_COURSES_EDX': True,
}
