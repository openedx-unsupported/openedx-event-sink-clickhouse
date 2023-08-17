"""Utility functions for event_sink_clickhouse."""
from importlib import import_module

from django.conf import settings


def get_model(model_setting):
    """Load a model from a setting."""
    MODEL_CONFIG = getattr(settings, "EVENT_SINK_CLICKHOUSE_MODEL_CONFIG", {})

    model_config = getattr(MODEL_CONFIG, model_setting, {})

    module = model_config.get("module")
    if not module:
        return None

    model_name = model_config.get("model")
    if not model_name:
        return None

    model = getattr(import_module(module), model_name)
    return model
