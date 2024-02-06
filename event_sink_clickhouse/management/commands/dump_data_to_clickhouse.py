"""
Management command for exporting the modulestore ClickHouse.

Example usages (see usage for more options):

    # Dump all objects published since last dump.
    # Use connection parameters from `settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG`:
    python manage.py cms dump_objects_to_clickhouse --object user_profile

    # Specify certain objects instead of dumping all of them.
    # Use connection parameters from `settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG`.
    python manage.py cms dump_objects_to_clickhouse --object user_profile --objects 123 124 125

    # Dump a limited number of objects to prevent stress on production systems
    python manage.py cms dump_objects_to_clickhouse --limit 1000
"""
import logging
from textwrap import dedent

from django.core.management.base import BaseCommand, CommandError

from event_sink_clickhouse.sinks.base_sink import ModelBaseSink
from event_sink_clickhouse.tasks import dump_data_to_clickhouse

log = logging.getLogger(__name__)


def dump_target_objects_to_clickhouse(
    sink=None,
    object_ids=[],
    objects_to_skip=[],
    force=False,
    limit=None,
    batch_size=1000,
):
    """
    Iterates through a list of objects in the ORN, serializes them to csv,
    then submits tasks to post them to ClickHouse.

    Arguments:
        force: serialize the objects even if they've been recently
            serialized

    Returns: two lists--one of the objects that had dump jobs queued for them
        and one of objects that did not.
    """

    submitted_objects = []
    skipped_objects = []

    index = 0
    objects_to_submit = []
    for object_id, should_be_dumped, reason in sink.fetch_target_items(
        object_ids, objects_to_skip, force
    ):
        log.info(f"Iteration {index}: {object_id}")
        index += 1

        if not should_be_dumped:
            skipped_objects.append(object_id)
            log.info(
                f"{sink.model} {index}: Skipping object {object_id}, reason: '{reason}'"
            )
        else:
            log.info(
                f"{sink.model} {index}: Submitting {object_id} for dump to ClickHouse, reason '{reason}'."
            )
            objects_to_submit.append(object_id)

            if len(objects_to_submit) % batch_size == 0:
                sink.dump(objects_to_submit, many=True)
                submitted_objects.extend(objects_to_submit)
                objects_to_submit = []

            submitted_objects.append(str(object_id))

            if limit and len(submitted_objects) == limit:
                log.info(
                    f"Limit of {limit} eligible objects has been reached, quitting!"
                )
                break

    return submitted_objects, skipped_objects


class Command(BaseCommand):
    """
    Dump objects to a ClickHouse instance.
    """

    help = dedent(__doc__).strip()

    def add_arguments(self, parser):
        parser.add_argument(
            "--url",
            type=str,
            help="the URL of the ClickHouse server",
        )
        parser.add_argument(
            "--username",
            type=str,
            help="the username of the ClickHouse user",
        )
        parser.add_argument(
            "--password",
            type=str,
            help="the password of the ClickHouse user",
        )
        parser.add_argument(
            "--database",
            type=str,
            help="the database in ClickHouse to connect to",
        )
        parser.add_argument(
            "--timeout_secs",
            type=int,
            help="timeout for ClickHouse requests, in seconds",
        )
        parser.add_argument(
            "--object",
            type=str,
            help="the type of object to dump",
        )
        parser.add_argument(
            "--ids",
            metavar="KEY",
            type=str,
            nargs="*",
            help="keys of objects to serialize; if omitted all objects in system are serialized",
        )
        parser.add_argument(
            "--ids_to_skip",
            metavar="KEY",
            type=str,
            nargs="*",
            help="keys of objects to NOT to serialize",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="dump all objects regardless of when they were last published",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="maximum number of objects to dump, cannot be used with '--ids' or '--force'",
        )
        parser.add_argument(
            "--batch_size",
            type=int,
            default=1000,
            help="number of objects to dump in a single batch",
        )

    def handle(self, *args, **options):
        """
        Iterates through each objects, serializes and saves them to clickhouse.
        """
        connection_overrides = {
            key: options[key]
            for key in ["url", "username", "password", "database", "timeout_secs"]
            if options[key]
        }

        ids = options["ids"] if options["ids"] else []
        ids_to_skip = options["ids_to_skip"] if options["ids_to_skip"] else []

        if options["limit"] is not None and int(options["limit"]) < 1:
            message = "'limit' must be greater than 0!"
            log.error(message)
            raise CommandError(message)

        if options["limit"] and options["force"]:
            message = (
                "The 'limit' option cannot be used with 'force' as running the "
                "command repeatedly will result in the same objects being dumped every time."
            )
            log.error(message)
            raise CommandError(message)

        if options["object"] is None:
            message = "You must specify an object type to dump!"
            log.error(message)
            raise CommandError(message)

        for cls in ModelBaseSink.__subclasses__():  # pragma: no cover
            if cls.model == options["object"]:
                sink = cls(connection_overrides, log)
                submitted_objects, skipped_objects = dump_target_objects_to_clickhouse(
                    sink,
                    [object_id.strip() for object_id in ids],
                    [object_id.strip() for object_id in ids_to_skip],
                    options["force"],
                    options["limit"],
                    options["batch_size"],
                )

                log.info(
                    "%d objects submitted for export to ClickHouse. %d objects skipped.",
                    len(submitted_objects),
                    len(skipped_objects),
                )

                if not submitted_objects:
                    log.info("No objects submitted for export to ClickHouse at all!")
                else:
                    log.info(  # pylint: disable=logging-not-lazy
                        "These objects were submitted for dump to ClickHouse successfully:\n\t"
                        + "\n\t".join(submitted_objects)
                    )
                break
