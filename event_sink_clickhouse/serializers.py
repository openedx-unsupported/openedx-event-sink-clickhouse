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
        """Meta classs for course overview serializer."""

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


class CourseBlockSerializer(BaseSinkSerializer):  # pylint: disable=abstract-method
    """Serializer for course block model."""

    org = serializers.CharField()
    course_key = serializers.SerializerMethodField()
    location = serializers.SerializerMethodField()
    display_name = serializers.SerializerMethodField()
    xblock_data_json = serializers.SerializerMethodField()
    order = serializers.IntegerField()
    edited_on = serializers.CharField()

    class Meta:
        """Meta class for course block serializer."""

        fields = [
            "org",
            "course_key",
            "location",
            "display_name",
            "xblock_data_json",
            "order",
            "edited_on",
            "dump_id",
            "time_last_dumped",
        ]

    def get_course_key(self, block):
        """Return the course key as a string."""
        return str(block.course_key)

    def get_location(self, block):
        """Return the location as a string."""
        return str(block.location)

    def get_display_name(self, block):
        """Return the display name as a string."""
        return str(block.display_name)


class XBlockRelationshipSerializer(
    BaseSinkSerializer
):  # pylint: disable=abstract-method
    """Serializer for the XBlockRelationship model."""

    course_key = serializers.CharField()
    parent_location = serializers.CharField()
    child_location = serializers.CharField()
    order = serializers.IntegerField()

    class Meta:
        """Meta class for XBlockRelationship serializer."""

        fields = [
            "course_key",
            "parent_location",
            "child_location",
            "order",
            "dump_id",
            "time_last_dumped",
        ]

    def to_representation(self, instance):
        """Return the representation of the XBlockRelationship instance."""
        return {
            "course_key": str(instance.course_key),
            "parent_location": str(instance.parent_location),
            "child_location": str(instance.child_location),
            "order": instance.order,
            "dump_id": instance.dump_id,
            "time_last_dumped": instance.time_last_dumped,
        }
