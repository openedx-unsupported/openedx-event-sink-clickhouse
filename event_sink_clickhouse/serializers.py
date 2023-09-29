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


class UserExternalIDSerializer(BaseSinkSerializer, serializers.ModelSerializer):
    """Serializer for user external ID events."""

    external_id_type = serializers.CharField(source="external_id_type.name")
    username = serializers.CharField(source="user.username")

    class Meta:
        """Meta class for user external ID serializer."""

        model = get_model("external_id")
        fields = [
            "external_user_id",
            "external_id_type",
            "username",
            "user_id",
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
            "advertised_start": getattr(overview, "advertised_start", ""),
            "announcement": getattr(overview, "announcement", ""),
            "lowest_passing_grade": float(
                getattr(overview, "lowest_passing_grade", 0.0)
            ),
            "invitation_only": getattr(overview, "invitation_only", ""),
            "max_student_enrollments_allowed": getattr(
                overview, "max_student_enrollments_allowed", None
            ),
            "effort": getattr(overview, "effort", ""),
            "enable_proctored_exams": getattr(overview, "enable_proctored_exams", ""),
            "entrance_exam_enabled": getattr(overview, "entrance_exam_enabled", ""),
            "external_id": getattr(overview, "external_id", ""),
            "language": getattr(overview, "language", ""),
        }
        return json.dumps(json_fields)

    def get_course_key(self, overview):
        """Return the course key as a string."""
        return str(overview.id)
