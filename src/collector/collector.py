import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Tuple

import pandas as pd

from src.collector.models import Resource

logger = logging.getLogger(__name__)


@dataclass
class Fields:
    ingress_src_fields: Tuple[str] = ("srcaddr",)
    ingress_dst_fields: Tuple[str] = ("dstaddr", "account_id", 'instance_id', 'interface_id', 'az_id', 'vpc_id')
    egress_src_fields: Tuple[str] = ("srcaddr", "account_id", 'instance_id', 'interface_id', 'az_id', 'vpc_id')
    egress_dst_fields: Tuple[str] = ("dstaddr",)


class LogFile:

    def __init__(self, log_path, ):
        self.log_path = log_path
        self.dt = datetime.strptime(
            self.log_path.rsplit('_', 2)[1], '%Y%m%dT%H%MZ'
        )
        self.log_format = 'parquet' if log_path.endswith('.parquet') else 'csv'
        self.df = self.load_parquet() if self.log_format == 'parquet' else self.load_csv()
        self._src_resource: list = None
        self._dst_resource: list = None
        self.resource_ips: set = set()

    def load_csv(self):
        raw = pd.read_csv(self.log_path, delimiter=" ", low_memory=False)
        replace_columns = {col: col.replace('-', '_') for col in raw.columns}
        raw.rename(columns=replace_columns, inplace=True)

        # remove no data
        result = raw[raw["bytes"] != '-'].copy()
        result['bytes'] = pd.to_numeric(result['bytes'], errors='coerce')
        result['packets'] = pd.to_numeric(result['packets'], errors='coerce')

        # replace '-' to None
        for field in result.columns.values:
            result[field] = result[field].replace({'-': None})

        return result

    def flow_direction_filter(self, df, egress=False) -> pd.DataFrame:
        direction = 'egress' if egress else 'ingress'
        return df[df['flow_direction'] == direction]

    def _make_resource(self, fields: Tuple[str], data: tuple) -> dict or None:
        # check ip is already exist

        result = {fields[n]: value if value != '-' else None for n, value in enumerate(data)}

        # field remapping
        address_field = 'srcaddr' if 'srcaddr' in fields else 'dstaddr'
        ip = result.pop(address_field)

        if 'interface_id' in fields:
            end_id = result.pop('interface_id')
            result['eni_id'] = end_id

        result['address'] = ip
        return result

    def _get_src_resource(self):
        ips = set()

        # remove duplicated ips
        ingress_filter = self.flow_direction_filter(self.df, egress=False)
        for _fields, _ in ingress_filter.groupby(list(Fields.ingress_dst_fields)):
            resource = self._make_resource(Fields.ingress_dst_fields, _fields)
            if resource['address'] in ips:
                continue
            ips.add(resource['address'])
            yield resource

        egress_filter = self.flow_direction_filter(self.df, egress=True)
        for _fields, _ in egress_filter.groupby(list(Fields.egress_src_fields)):
            resource = self._make_resource(Fields.egress_src_fields, _fields)
            if resource['address'] in ips:
                continue
            ips.add(resource['address'])
            yield resource

    def _to_dict(self, df) -> list:
        return df.to_dict(orient='records')

    def load_parquet(self):
        raise NotImplementedError()

    # generate resource
    def get_src_resource(self):
        if not self._src_resource:
            self._src_resource = list(self._get_src_resource())
        return self._src_resource

    def get_dst_resource(self):
        if not self._dst_resource:
            self._dst_resource = list(self._get_dst_resource())
        return self._dst_resource

    def _get_dst_resource(self):
        # add destination resource
        ips = set()
        ingress_filter = self.flow_direction_filter(self.df, egress=False)
        for _fields, _ in ingress_filter.groupby(list(Fields.ingress_src_fields)):
            resource = self._make_resource(Fields.ingress_src_fields, _fields)
            if resource['address'] in ips:
                continue
            ips.add(resource['address'])
            yield resource

        egress_filter = self.flow_direction_filter(self.df, egress=True)
        for _fields, _ in egress_filter.groupby(list(Fields.egress_dst_fields)):
            resource = self._make_resource(Fields.egress_dst_fields, _fields)
            if resource['address'] in ips:
                continue
            ips.add(resource['address'])
            yield resource

    def ingest_resource(self):
        logger.info('start ingest src resource')
        print('start ingest src resource')
        src_resource = self.get_src_resource()
        Resource.create_or_update(*src_resource)
        print('finish ingest src resource')
        logger.info('finish ingest src resource')

        logger.info('start ingest dst resource')
        print('start ingest dst resource')
        duplicated_ips = {r['address'] for r in src_resource}
        dst_resource = (r for r in self.get_dst_resource() if r['address'] not in duplicated_ips)
        Resource.create_or_update(*dst_resource)
        logger.info('finish ingest dst resource')
        print('finish ingest dst resource')

    # generate flow
    def get_records(self):
        ingress_filter = self.flow_direction_filter(self.df, egress=False)

        fields = [
            'flow_direction', 'srcaddr', 'dstaddr', 'interface_id', 'az_id', 'protocol', 'pkt_srcaddr', 'pkt_dstaddr',
            'pkt_dst_aws_service', 'pkt_src_aws_service', 'traffic_path']
        grouped = self.df.groupby(fields, dropna=False).agg({'bytes': 'sum', "packets": 'sum'})
        for groups, aggregated in grouped.iterrows():
            data = {key: groups[index] for index, key in enumerate(fields)}
            data['bytes'] = aggregated['bytes']
            data['packets'] = aggregated['packets']
            data['flow_at'] = self.dt
            yield data

    # def logs(self):
    #     loader = self.load_parquet if self.log_format == 'parquet' else self.load_csv
    #     for data in loader():
    #         yield FlowRecord(data)
    #


