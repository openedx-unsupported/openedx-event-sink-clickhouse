"""
Signal handler functions, mapped to specific signals in apps.py.
"""
from event_sink_clickhouse.sinks.utils import get_model

from django.db.models.signals import post_save
from django.dispatch import receiver


def receive_course_publish(sender, course_key, **kwargs):  # pylint: disable=unused-argument  # pragma: no cover
    """
    Receives COURSE_PUBLISHED signal and queues the dump job.
    """
    # import here, because signal is registered at startup, but items in tasks are not yet able to be loaded
    from event_sink_clickhouse.tasks import dump_course_to_clickhouse  # pylint: disable=import-outside-toplevel

    dump_course_to_clickhouse.delay(str(course_key))

@receiver(post_save, sender=get_model("user_profile"))
def on_user_profile_updated(sender, instance, **kwargs):  # pylint: disable=unused-argument  # pragma: no cover
    """
    Receives post save signal and queues the dump job.
    """
    # import here, because signal is registered at startup, but items in tasks are not yet able to be loaded
    from event_sink_clickhouse.tasks import dump_user_profile_to_clickhouse  # pylint: disable=import-outside-toplevel

    dump_user_profile_to_clickhouse.delay(instance.id)
