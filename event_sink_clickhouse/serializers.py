from rest_framework import serializers

import uuid
from django.utils import timezone

from event_sink_clickhouse.utils import get_model


class BaseSinkSerializer(serializers.Serializer):
    dump_id = serializers.SerializerMethodField()
    dump_timestamp = serializers.SerializerMethodField()

    class Meta:
        fields = [
            "dump_id",
            "dump_timestamp",
        ]

    def get_dump_id(self, instance):
        return uuid.uuid4()

    def get_dump_timestamp(self, instance):
        return timezone.now()


class UserProfileSerializer(BaseSinkSerializer, serializers.ModelSerializer):
    class Meta:
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
