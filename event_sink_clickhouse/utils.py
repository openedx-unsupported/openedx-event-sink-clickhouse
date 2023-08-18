"""Utility functions for event_sink_clickhouse."""
from importlib import import_module

from django.conf import settings


import logging

log = logging.getLogger(__name__)

def get_model(model_setting):
    """Load a model from a setting."""
    MODEL_CONFIG = getattr(settings, "EVENT_SINK_CLICKHOUSE_MODEL_CONFIG", {})

    model_config = MODEL_CONFIG.get(model_setting)

    module = model_config.get("module")
    if not module:
        return None

    model_name = model_config.get("model")
    if not model_name:
        return None

    try:
        model = getattr(import_module(module), model_name)
        return model
    except (ImportError, AttributeError, ModuleNotFoundError):
        log.error("Unable to load model %s.%s", module, model_name)

    return None
