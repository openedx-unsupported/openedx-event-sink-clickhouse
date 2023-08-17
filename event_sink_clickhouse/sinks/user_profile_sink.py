from event_sink_clickhouse.sinks.base_sink import ModelBaseSink, ItemSerializer

class UserProfileSerializer(ItemSerializer):
    """
    Serializer for user profile events
    """
    def serialize_item(self, user_profile):
        return {
            "id": user_profile.id,
            "name": user_profile.name,
            "meta": user_profile.meta,
            "courseware": user_profile.courseware,
            "language": user_profile.language,
            "location": user_profile.location,
            "year_of_birth": user_profile.year_of_birth,
            "gender": user_profile.gender,
            "level_of_education": user_profile.level_of_education,
            "mailing_address": user_profile.mailing_address,
            "city": user_profile.city,
            "country": user_profile.country,
            "state": user_profile.state,
            "goals": user_profile.goals,
            "bio": user_profile.bio,
            "profile_image_uploaded_at": user_profile.profile_image_uploaded_at,
            "phone_number": user_profile.phone_number,
        }


class UserProfileSink(ModelBaseSink):
    """
    Sink for user profile events
    """
    model = "user_profile"
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
    ]
    unique_key= "id"
    clickhouse_table_name = "user_profile"
    timestamp_field = "time_last_dumped"
    name = "User Profile"
    serializer_class = UserProfileSerializer
