# -*- coding: utf-8 -*-

from . import  row_event


class EventWrapper(object):

    __event_map = {
        "update": row_event.UpdateRowEvent,
        "insert": row_event.WriteRowEvent,
        "delete": row_event.DeleteRowEvent,
    }

    def __init__(self,
                 changes,
                 allowed_events,
                 only_tables,
                 ignored_tables,
                 only_schemas,
                 ignored_schemas,):

        self.events = []
        for change in changes:

            event_class = self.__event_map.get(
                change["kind"])
            if event_class not in allowed_events:
                continue
            table = change["table"]
            if only_tables is not None and table not in only_tables:
                continue
            elif ignored_tables is not None and table in ignored_tables:
                continue
            schema = change["schema"]
            if only_schemas is not None and schema not in only_schemas:
                continue
            elif ignored_schemas is not None and schema in ignored_tables:
                continue

            row_event = event_class(
                pkt=change
            )
            self.events.append(row_event)
