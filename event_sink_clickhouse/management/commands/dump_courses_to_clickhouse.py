"""
Management command for exporting the modulestore ClickHouse.

Example usages (see usage for more options):

    # Dump all courses published since last dump.
    # Use connection parameters from `settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG`:
    python manage.py cms dump_courses_to_clickhouse

    # Dump all courses published since last dump.
    # Use custom connection parameters to send to a different ClickHouse instance:
    python manage.py cms dump_courses_to_clickhouse --host localhost \
      --user user --password password --database research_db --

    # Specify certain courses instead of dumping all of them.
    # Use connection parameters from `settings.EVENT_SINK_CLICKHOUSE_BACKEND_CONFIG`.
    python manage.py cms dump_courses_to_clickhouse --courses 'course-v1:A+B+1' 'course-v1:A+B+2'

    # Dump a limited number of courses to prevent stress on production systems
    python manage.py cms dump_courses_to_clickhouse --limit 1000
"""
import logging
from textwrap import dedent

from django.core.management.base import BaseCommand, CommandError
from edx_django_utils.cache import RequestCache

from event_sink_clickhouse.sinks.course_published import CourseOverviewSink
from event_sink_clickhouse.tasks import dump_course_to_clickhouse

log = logging.getLogger(__name__)


def dump_target_courses_to_clickhouse(
    connection_overrides=None,
    course_keys=None,
    courses_to_skip=None,
    force=None,
    limit=None,
):
    """
    Iterates through a list of courses in a modulestore, serializes them to csv,
    then submits tasks to post them to ClickHouse.

    Arguments:
        force: serialize the courses even if they've been recently
            serialized

    Returns: two lists--one of the courses that had dump jobs queued for them
        and one of courses that did not.
    """
    sink = CourseOverviewSink(connection_overrides, log)

    submitted_courses = []
    skipped_courses = []

    index = 0
    for course_key, should_be_dumped, reason in sink.fetch_target_items(
        course_keys, courses_to_skip, force
    ):
        log.info(f"Iteration {index}: {course_key}")
        index += 1

        if not should_be_dumped:
            skipped_courses.append(course_key)
            log.info(
                f"Course {index}: Skipping course {course_key}, reason: '{reason}'"
            )
        else:
            # RequestCache is a local memory cache used in modulestore for performance reasons.
            # Normally it is cleared at the end of every request, but in this command it will
            # continue to grow until the command is done. To prevent excessive memory consumption
            # we clear it every time we dump a course.
            RequestCache.clear_all_namespaces()

            log.info(
                f"Course {index}: Submitting {course_key} for dump to ClickHouse, reason '{reason}'."
            )

            dump_course_to_clickhouse.apply_async(
                kwargs={
                    "course_key_string": str(course_key),
                    "connection_overrides": connection_overrides,
                }
            )

            submitted_courses.append(str(course_key))

            if limit and len(submitted_courses) == limit:
                log.info(
                    f"Limit of {limit} eligible course has been reached, quitting!"
                )
                break

    return submitted_courses, skipped_courses


class Command(BaseCommand):
    """
    Dump course block and relationship data to a ClickHouse instance.
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
            "--courses",
            metavar="KEY",
            type=str,
            nargs="*",
            help="keys of courses to serialize; if omitted all courses in system are serialized",
        )
        parser.add_argument(
            "--courses_to_skip",
            metavar="KEY",
            type=str,
            nargs="*",
            help="keys of courses to NOT to serialize",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="dump all courses regardless of when they were last published",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="maximum number of courses to dump, cannot be used with '--courses' or '--force'",
        )

    def handle(self, *args, **options):
        """
        Iterates through each course, serializes them into graphs, and saves
        those graphs to clickhouse.
        """
        connection_overrides = {
            key: options[key]
            for key in ["url", "username", "password", "database", "timeout_secs"]
            if options[key]
        }

        courses = options["courses"] if options["courses"] else []
        courses_to_skip = (
            options["courses_to_skip"] if options["courses_to_skip"] else []
        )

        if options["limit"] is not None and int(options["limit"]) < 1:
            message = "'limit' must be greater than 0!"
            log.error(message)
            raise CommandError(message)

        if options["limit"] and options["force"]:
            message = (
                "The 'limit' option cannot be used with 'force' as running the "
                "command repeatedly will result in the same courses being dumped every time."
            )
            log.error(message)
            raise CommandError(message)

        submitted_courses, skipped_courses = dump_target_courses_to_clickhouse(
            connection_overrides,
            [course_key.strip() for course_key in courses],
            [course_key.strip() for course_key in courses_to_skip],
            options["force"],
            options["limit"],
        )

        log.info(
            "%d courses submitted for export to ClickHouse. %d courses skipped.",
            len(submitted_courses),
            len(skipped_courses),
        )

        if not submitted_courses:
            log.info("No courses submitted for export to ClickHouse at all!")
        else:
            log.info(  # pylint: disable=logging-not-lazy
                "These courses were submitted for dump to ClickHouse successfully:\n\t"
                + "\n\t".join(submitted_courses)
            )
