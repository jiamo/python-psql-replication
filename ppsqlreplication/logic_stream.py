# -*- coding: utf-8 -*-

import psycopg2
import json
import psycopg2.extras
from .packet import EventWrapper
from .row_event import (
    UpdateRowEvent, WriteRowEvent, DeleteRowEvent)


class LogicStreamReader(object):

    def __init__(self,
                 connection_settings,
                 only_events=None,
                 ignored_events=None,
                 only_tables=None,
                 ignored_tables=None,
                 only_schemas=None,
                 ignored_schemas=None,
                 start_lsn=0,
                 slot_name=None):

        self.slot_name = slot_name
        self.none_times = 0  # we compute the total sleep in logic_stream
        self.connection_settings = connection_settings
        self.stream_connection = None
        self.cur = None
        self.start_lsn = start_lsn
        self.data_start = start_lsn
        self.flush_lsn = start_lsn
        self.connected_stream = False
        self.only_tables = only_tables
        self.ignored_tables = ignored_tables
        self.only_schemas = only_schemas
        self.ignored_schemas = ignored_schemas
        self.allowed_events = self.allowed_event_list(
            only_events, ignored_events)

    def close(self):
        if self.connected_stream:
            self.stream_connection.close()
            self.connected_stream = False

    def connect_to_stream(self):
        self.stream_connection = psycopg2.connect(
            self.connection_settings,
            connection_factory=psycopg2.extras.LogicalReplicationConnection
        )

        self.cur = self.stream_connection.cursor()
        self.cur.start_replication(
            slot_name=self.slot_name,
            decode=True,
            start_lsn=self.flush_lsn,   # first we debug don't flush
        )

        self.connected_stream = True

    def send_feedback(self, lsn=None):
        if not self.connected_stream:
            self.connect_to_stream()
        if lsn is None:
            lsn = self.flush_lsn
        else:
            # update it
            self.flush_lsn = lsn    # here we update lsn
        self.cur.send_feedback(flush_lsn=lsn, reply=True)

    def fetchone(self):

        while True:

            if not self.connected_stream:
                self.connect_to_stream()

            try:
                pkt = self.cur.read_message()
            except psycopg2.DatabaseError as error:
                self.stream_connection.close()
                self.connected_stream = False
                continue

            if not pkt:
                # we don't have any data, first send some feedback
                # but when there always no data.
                # the client don't have chance to send_feedback
                # does we need to seed feedback?
                # If we got 30 None we send back
                self.none_times += 1
                if self.none_times > 30:
                    self.send_feedback()
                    self.none_times = 0
                return None

            else:
                self.data_start = pkt.data_start
                payload_json = json.loads(pkt.payload)
                changes = payload_json["change"]

            if changes:
                wraper = EventWrapper(
                    changes,
                    self.allowed_events,
                    self.only_tables,
                    self.ignored_tables,
                    self.only_schemas,
                    self.ignored_schemas)

                if not wraper.events:
                    continue
                return wraper.events

            else:
                # seem like last wal have finished we send it
                self.send_feedback(self.data_start)

    def allowed_event_list(self, only_events, ignored_events):
        if only_events is not None:
            events = set(only_events)
        else:
            events = {
                UpdateRowEvent,
                WriteRowEvent,
                DeleteRowEvent,
            }
        if ignored_events is not None:
            for e in ignored_events:
                events.remove(e)

        return frozenset(events)

    def __iter__(self):
        return iter(self.fetchone, None)
