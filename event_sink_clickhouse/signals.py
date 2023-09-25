"""
Signal handler functions, mapped to specific signals in apps.py.
"""
from django.db.models.signals import post_save
from django.dispatch import receiver

from event_sink_clickhouse.sinks.external_id_sink import ExternalIDSInk
from event_sink_clickhouse.utils import get_model


def receive_course_publish(  # pylint: disable=unused-argument  # pragma: no cover
    sender, course_key, **kwargs
):
    """
    Receives COURSE_PUBLISHED signal and queues the dump job.
    """
    # import here, because signal is registered at startup, but items in tasks are not yet able to be loaded
    from event_sink_clickhouse.tasks import dump_course_to_clickhouse  # pylint: disable=import-outside-toplevel

    dump_course_to_clickhouse.delay(str(course_key))


@receiver(post_save, sender=get_model("user_profile"))
def on_user_profile_updated(  # pylint: disable=unused-argument  # pragma: no cover
    sender, instance, **kwargs
):
    """
    Receives post save signal and queues the dump job.
    """
    # import here, because signal is registered at startup, but items in tasks are not yet able to be loaded
    from event_sink_clickhouse.tasks import dump_user_profile_to_clickhouse  # pylint: disable=import-outside-toplevel

    dump_user_profile_to_clickhouse.delay(instance.id)


@receiver(post_save, sender=get_model("external_id"))
def on_externalid_saved(  # pylint: disable=unused-argument  # pragma: no cover
    sender, instance, **kwargs
):
    """
    Receives post save signal and queues the dump job.
    """
    # import here, because signal is registered at startup, but items in tasks are not yet able to be loaded
    from event_sink_clickhouse.tasks import dump_data_to_clickhouse  # pylint: disable=import-outside-toplevel

    sink = ExternalIDSInk(None, None)
    dump_data_to_clickhouse.delay(
        sink_module=sink.__module__,
        sink_name=sink.__class__.__name__,
        object_id=str(instance.id),
    )
