# coding=utf-8
# TODO: make the next two lines unnecessary
# pylint: disable=line-too-long
# pylint: disable=missing-docstring
import logging
import sys
import datetime

import graphitesend
from graphitesend.formatter import GraphiteStructuredFormatter

from ...common.interfaces import AbstractPlugin
from ...common.interfaces import MonitoringDataListener
from ...common.interfaces import AggregateResultListener

logger = logging.getLogger(__name__)  # pylint: disable=C0103


class Plugin(AbstractPlugin, AggregateResultListener, MonitoringDataListener):

    SECTION = 'graphite'

    def __init__(self, core, cfg, cfg_updater):
        AbstractPlugin.__init__(self, core, cfg, cfg_updater)
        self.tank_tag = self.get_option("tank_tag")
        self.client = graphitesend.init(
            graphite_server=self.get_option("graphite_server"),
            graphite_port=self.get_option("graphite_port")
        )
        self.prefix = self.get_option("prefix")

    def start_test(self):
        self.start_time = datetime.datetime.now()

    def end_test(self, retcode):
        self.end_time = datetime.datetime.now() + datetime.timedelta(minutes=1)
        return retcode

    def prepare_test(self):
        self.core.job.subscribe_plugin(self)

    def monitoring_data(self, data_list):
        if self.client:
            for chunk in data_list:
                self._send_monitoring(chunk)

    def on_aggregated_data(self, data, stats):
        if self.client:
            timestamp = int(data["ts"])
            overall_fmt = GraphiteStructuredFormatter(
                prefix=self.prefix,
                group=self.tank_tag,
                suffix="overall_quantiles")
            self.client.send_dict({
                    'q' + str(q): value / 1000.0
                    for q, value in zip(data["overall"]["interval_real"]["q"]["q"],
                                        data["overall"]["interval_real"]["q"]["value"])
                }, timestamp=timestamp, formatter=overall_fmt)

            overall_meta_fmt = GraphiteStructuredFormatter(
                prefix=self.prefix,
                group=self.tank_tag,
                suffix="overall_meta")
            self.client.send_dict({
                    "active_threads": stats["metrics"]["instances"],
                    "RPS": data["overall"]["interval_real"]["len"],
                    "planned_requests": float(stats["metrics"]["reqps"]),
                }, timestamp=timestamp, formatter=overall_meta_fmt)

            net_codes_fmt = GraphiteStructuredFormatter(
                prefix=self.prefix,
                group=self.tank_tag,
                suffix="net_codes")
            self.client.send_dict({
                    str(code): int(cnt)
                    for code, cnt in data["overall"]["net_code"]["count"].iteritems()
                }, timestamp=timestamp, formatter=net_codes_fmt)

            proto_codes_fmt = GraphiteStructuredFormatter(
                prefix=self.prefix,
                group=self.tank_tag,
                suffix="proto_codes")
            self.client.send_dict({
                    str(code): int(cnt)
                    for code, cnt in data["overall"]["proto_code"]["count"].iteritems()
                }, timestamp=timestamp, formatter=proto_codes_fmt)

    def _send_monitoring(self, data):
        for element in data:
            ts = element["timestamp"]
            for host, host_data in element["data"].iteritems():
                fmt = GraphiteStructuredFormatter(prefix=self.prefix, group=self.tank_tag, suffix=host)
                self.client.send_dict({
                        metric: value
                        for metric, value in host_data["metrics"].iteritems()
                    }, timestamp=ts, formatter=fmt)

