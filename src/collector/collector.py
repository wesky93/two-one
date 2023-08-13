from dataclasses import dataclass
from datetime import datetime
from typing import Tuple

import pandas as pd


@dataclass
class Fields:
    ingress_src_fields: Tuple[str] = (
        "srcaddr", 'pkt_src_aws_service',)
    ingress_dst_fields: Tuple[str] = (
        "dstaddr", "account_id", 'instance_id', 'interface_id', 'pkt_dst_aws_service', 'az_id')
    egress_src_fields: Tuple[str] = (
        "srcaddr", "account_id", 'instance_id', 'interface_id', 'pkt_src_aws_service', 'az_id')
    egress_dst_fields: Tuple[str] = (
        "dstaddr", 'pkt_dst_aws_service',)


class LogFile:
    def __init__(self, log_path, ):
        self.log_path = log_path
        self.dt = datetime.strptime(
            self.log_path.rsplit('_', 2)[1], '%Y%m%dT%H%MZ'
        )
        self.log_format = 'parquet' if log_path.endswith('.parquet') else 'csv'
        self.df = self.load_parquet() if self.log_format == 'parquet' else self.load_csv()
        self.ingress_ips: set = set()

    def load_csv(self):
        raw = pd.read_csv(self.log_path, delimiter=" ", low_memory=False)
        replace_columns = {col: col.replace('-', '_') for col in raw.columns}
        raw.rename(columns=replace_columns, inplace=True)

        # remove no data
        result = raw[raw["bytes"] != '-']
        return result

    def flow_direction_filter(self, df, egress=False) -> pd.DataFrame:
        direction = 'egress' if egress else 'ingress'
        return df[df['flow_direction'] == direction]

    def _make_resource(self, fields: Tuple[str], data: tuple) -> dict or None:
        result = {fields[n]: value if value != '-' else None for n, value in enumerate(data)}
        ip = data[0]
        if ip in self.ingress_ips:
            return None
        self.ingress_ips.add(data[0])
        return result

    def get_resource(self):
        ingress_filter = self.flow_direction_filter(self.df, egress=False)
        for _fields, _ in ingress_filter.groupby(list(Fields.ingress_dst_fields)):
            if resource := self._make_resource(Fields.ingress_dst_fields, _fields):
                yield resource

        egress_filter = self.flow_direction_filter(self.df, egress=True)
        for _fields, _ in egress_filter.groupby(list(Fields.egress_dst_fields)):
            if resource := self._make_resource(Fields.egress_src_fields, _fields):
                yield resource

    def _to_dict(self, df) -> list:
        return df.to_dict(orient='records')

    def load_parquet(self):
        raise NotImplementedError()

    # def logs(self):
    #     loader = self.load_parquet if self.log_format == 'parquet' else self.load_csv
    #     for data in loader():
    #         yield FlowRecord(data)
    #


if __name__ == '__main__':
    path = '/Users/sinsky/code/two-one/data/sample/AWSLogs/445363019552/vpcflowlogs/ap-northeast-2/2023/06/28/07/445363019552_vpcflowlogs_ap-northeast-2_fl-0ca6e6b2f6070ce84_20230628T0735Z_6626d0f9.log.gz'
    log_file = LogFile(path)
    print(log_file.dt)

    print(log_file.df.head())
    print(len(log_file.df))
    for data in log_file.get_resource():
        print('test')
        print(data)
        break
