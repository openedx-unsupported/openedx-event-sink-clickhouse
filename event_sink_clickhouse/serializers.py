"""Django serializers for the event_sink_clickhouse app."""
import json
import uuid

from django.utils import timezone
from rest_framework import serializers

from event_sink_clickhouse.utils import get_model


class BaseSinkSerializer(serializers.Serializer):  # pylint: disable=abstract-method
    """Base sink serializer for ClickHouse."""

    dump_id = serializers.SerializerMethodField()
    time_last_dumped = serializers.SerializerMethodField()

    class Meta:
        """Meta class for base sink serializer."""

        fields = [
            "dump_id",
            "time_last_dumped",
        ]

    def get_dump_id(self, instance):  # pylint: disable=unused-argument
        """Return a unique ID for the dump."""
        return uuid.uuid4()

    def get_time_last_dumped(self, instance):  # pylint: disable=unused-argument
        """Return the timestamp for the dump."""
        return timezone.now()


class UserProfileSerializer(BaseSinkSerializer, serializers.ModelSerializer):
    """Serializer for user profile events."""

    class Meta:
        """Meta class for user profile serializer."""

        model = get_model("user_profile")
        fields = [
            "id",
            "user_id",
            "name",
            "meta",
            "courseware",
            "language",
            "location",
            "year_of_birth",
            "gender",
            "level_of_education",
            "mailing_address",
            "city",
            "country",
            "state",
            "goals",
            "bio",
            "profile_image_uploaded_at",
            "phone_number",
            "dump_id",
            "time_last_dumped",
        ]


class CourseOverviewSerializer(BaseSinkSerializer, serializers.ModelSerializer):
    """Serializer for course overview events."""

    course_data_json = serializers.SerializerMethodField()
    course_key = serializers.SerializerMethodField()
    course_start = serializers.CharField(source="start")
    course_end = serializers.CharField(source="end")

    class Meta:
        """Meta classes for course overview serializer."""

        model = get_model("course_overviews")
        fields = [
            "org",
            "course_key",
            "display_name",
            "course_start",
            "course_end",
            "enrollment_start",
            "enrollment_end",
            "self_paced",
            "course_data_json",
            "created",
            "modified",
            "dump_id",
            "time_last_dumped",
        ]

    def get_course_data_json(self, overview):
        """Return the course data as a JSON string."""
        json_fields = {
            "advertised_start": overview.advertised_start,
            "announcement": overview.announcement,
            "lowest_passing_grade": float(overview.lowest_passing_grade),
            "invitation_only": overview.invitation_only,
            "max_student_enrollments_allowed": overview.max_student_enrollments_allowed,
            "effort": overview.effort,
            "enable_proctored_exams": overview.enable_proctored_exams,
            "entrance_exam_enabled": overview.entrance_exam_enabled,
            "external_id": overview.external_id,
            "language": overview.language,
        }
        return json.dumps(json_fields)

    def get_course_key(self, overview):
        """Return the course key as a string."""
        return str(overview.id)
