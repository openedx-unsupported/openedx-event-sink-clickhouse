"""Django serializers for the event_sink_clickhouse app."""
import uuid

from django.utils import timezone
from rest_framework import serializers

from event_sink_clickhouse.utils import get_model


class BaseSinkSerializer(serializers.Serializer):  # pylint: disable=abstract-method
    """Base sink serializer for ClickHouse."""

    dump_id = serializers.SerializerMethodField()
    dump_timestamp = serializers.SerializerMethodField()

    class Meta:
        """Meta class for base sink serializer."""

        fields = [
            "dump_id",
            "dump_timestamp",
        ]

    def get_dump_id(self, instance):  # pylint: disable=unused-argument
        """Return a unique ID for the dump."""
        return uuid.uuid4()

    def get_dump_timestamp(self, instance):  # pylint: disable=unused-argument
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
            "dump_timestamp",
        ]
