# -*- coding: utf-8 -*-


class RowEvent():
    def __init__(self, pkt=None):
        self._row = None
        self.pkt = pkt
        self.schema = self.pkt["schema"]
        self.table = self.pkt["table"]


class DeleteRowEvent(RowEvent):

    def __init__(self, **kwargs):
        super(DeleteRowEvent, self).__init__(**kwargs)

    def _fetch_row(self):
        self._row = {}
        data = {}
        for column_data in zip(self.pkt["oldkeys"]["keynames"],
                               self.pkt["oldkeys"]["keyvalues"]):
            data[column_data[0]] = column_data[1]
        self._row["values"] = data

    @property
    def row(self):
        if self._row is None:
            self._fetch_row()
        return self._row


class WriteRowEvent(RowEvent):

    def __init__(self, **kwargs):
        super(WriteRowEvent, self).__init__(**kwargs)

    def _fetch_row(self):
        self._row = {}
        data = {}
        for column_data in zip(self.pkt["columnnames"],
                               self.pkt["columnvalues"]):
            data[column_data[0]] = column_data[1]
        self._row["values"] = data

    @property
    def row(self):
        if self._row is None:
            self._fetch_row()
        return self._row


class UpdateRowEvent(RowEvent):

    def __init__(self, **kwargs):
        super(UpdateRowEvent, self).__init__(**kwargs)

    def _fetch_row(self):
        self._row = {}
        after_value = {}
        for column_data in zip(self.pkt["columnnames"],
                               self.pkt["columnvalues"]):
            after_value[column_data[0]] = column_data[1]

        # just don't keep ddl old row always have the some column with new row
        before_value = {}
        for column_data in zip(self.pkt["oldkeys"]["keynames"],
                               self.pkt["oldkeys"]["keyvalues"]):
            before_value[column_data[0]] = column_data[1]
        self._row["after_values"] = after_value
        self._row["before_values"] = before_value


    @property
    def row(self):
        if self._row is None:
            self._fetch_row()
        return self._row

