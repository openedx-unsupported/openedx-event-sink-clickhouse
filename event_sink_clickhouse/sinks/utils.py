from importlib import import_module

from django.conf import settings


def get_model(model_name):
    model_config = getattr(settings, "EVENT_SINK_CLICKHOUSE_MODEL_CONFIG").get(model_name)
    module = model_config["module"]
    model_name = model_config["model"]
    model = getattr(import_module(module), model_name)
    return model
