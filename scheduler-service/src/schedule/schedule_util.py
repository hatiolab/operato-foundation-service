from datetime import datetime, timezone
from croniter import croniter


def calculate_cron_unit(cron_element):
    slash_splits = cron_element.split("/")

    conv_seconds = 0
    if len(slash_splits) == 2:
        conv_seconds = int(slash_splits[1])
    elif cron_element == "*":
        conv_seconds = 1
    else:
        conv_seconds = int(cron_element)

    return conv_seconds
