import os

from neomodel import config, StructuredNode, StringProperty, IntegerProperty, RelationshipTo, \
    StructuredRel, ArrayProperty, JSONProperty

NEO4J_USERNAME = os.environ.get('NEO4J_USERNAME', 'neo4j')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD')
NEO4J_HOST = os.environ.get('NEO4J_HOST', 'localhost')
NEO4J_PORT = os.environ.get('NEO4J_PORT', 7687)

config.AUTO_INSTALL_LABELS = True
config.DATABASE_URL = f'bolt://{NEO4J_USERNAME}:{NEO4J_PASSWORD}@{NEO4J_HOST}:{NEO4J_PORT}'

PROTOCOL_MAP = {
    0: "HOPOPT",
    1: "ICMP",
    2: "IGMP",
    3: "GGP",
    4: "IPv4",
    5: "ST",
    6: "TCP",
    7: "CBT",
    8: "EGP",
    9: "IGP",
    10: "BBN-RCC-MON",
    11: "NVP-II",
    12: "PUP",
    13: "ARGUS (deprecated)",
    14: "EMCON",
    15: "XNET",
    16: "CHAOS",
    17: "UDP",
    18: "MUX",
    19: "DCN-MEAS",
    20: "HMP",
    21: "PRM",
    22: "XNS-IDP",
    23: "TRUNK-1",
    24: "TRUNK-2",
    25: "LEAF-1",
    26: "LEAF-2",
    27: "RDP",
    28: "IRTP",
    29: "ISO-TP4",
    30: "NETBLT",
    31: "MFE-NSP",
    32: "MERIT-INP",
    33: "DCCP",
    34: "3PC",
    35: "IDPR",
    36: "XTP",
    37: "DDP",
    38: "IDPR-CMTP",
    39: "TP++",
    40: "IL",
    41: "IPv6",
    42: "SDRP",
    43: "IPv6-Route",
    44: "IPv6-Frag",
    45: "IDRP",
    46: "RSVP",
    47: "GRE",
    48: "DSR",
    49: "BNA",
    50: "ESP",
    51: "AH",
    52: "I-NLSP",
    53: "SWIPE (deprecated)",
    54: "NARP",
    55: "MOBILE",
    56: "TLSP",
    57: "SKIP",
    58: "IPv6-ICMP",
    59: "IPv6-NoNxt",
    60: "IPv6-Opts",
    61: "Any-Host-Internal-Protocol",
    62: "CFTP",
    63: "Any-Local-Network",
    64: "SAT-EXPAK",
    65: "KRYPTOLAN",
    66: "RVD",
    67: "IPPC",
    68: "Any-Distributed-File-System",
    69: "SAT-MON",
    70: "VISA",
    71: "IPCV",
    72: "CPNX",
    73: "CPHB",
    74: "WSN",
    75: "PVP",
    76: "BR-SAT-MON",
    77: "SUN-ND",
    78: "WB-MON",
    79: "WB-EXPAK",
    80: "ISO-IP",
    81: "VMTP",
    82: "SECURE-VMTP",
    83: "VINES",
    84: "IPTM",
    85: "NSFNET-IGP",
    86: "DGP",
    87: "TCF",
    88: "EIGRP",
    89: "OSPFIGP",
    90: "Sprite-RPC",
    91: "LARP",
    92: "MTP",
    93: "AX.25",
    94: "IPIP",
    95: "MICP (deprecated)",
    96: "SCC-SP",
    97: "ETHERIP",
    98: "ENCAP",
    99: "Any-private-encryption-scheme",
    100: "GMTP",
    101: "IFMP",
    102: "PNNI",
    103: "PIM",
    104: "ARIS",
    105: "SCPS",
    106: "QNX",
    107: "A/N",
    108: "IPComp",
    109: "SNP",
    110: "Compaq-Peer",
    111: "IPX-in-IP",
    112: "VRRP",
    113: "PGM",
    114: "Any-0-hop-protocol",
    115: "L2TP",
    116: "DDX",
    117: "IATP",
    118: "STP",
    119: "SRP",
    120: "UTI",
    121: "SMP",
    122: "SM (deprecated)",
    123: "PTP",
    124: "ISIS over IPv4",
    125: "FIRE",
    126: "CRTP",
    127: "CRUDP",
    128: "SSCOPMCE",
    129: "IPLT",
    130: "SPS",
    131: "PIPE",
    132: "SCTP",
    133: "FC",
    134: "RSVP-E2E-IGNORE",
    135: "Mobility Header",
    136: "UDPLite",
    137: "MPLS-in-IP",
    138: "manet",
    139: "HIP",
    140: "Shim6",
    141: "WESP",
    142: "ROHC",
    143: "Ethernet",
    144: "AGGFRAG",
    145: "NSH",
}


class SimpleFlowRel(StructuredRel):
    """
    효율 적인 분석을 위해 출발, 도착지 포트를 합산합니다.
    """
    type = StringProperty(required=True, index=True)  # ingress, egress
    bytes = IntegerProperty(index=True)
    packets = IntegerProperty(index=True)
    traffic_path = IntegerProperty(index=True)
    protocol = StringProperty(index=True)
    pkt_srcaddr = StringProperty()  # srcaddr과 pkt srcaddr이 다를 경우만 저장
    pkt_dstaddr = StringProperty()  # dstaddr과 pkt dstaddr이 다를 경우만 저장
    pkt_src_svc = StringProperty()
    pkt_dst_svc = StringProperty()


class FlowRel(StructuredRel):
    src_port = IntegerProperty(required=True)
    dst_port = IntegerProperty(required=True)


class Resource(StructuredNode):
    address = StringProperty(required=True)
    tgw_id = StringProperty(index=True, default=None)
    vpc_id = StringProperty(index=True, default=None)
    az_id = StringProperty(index=True, default=None)
    subnet_id = StringProperty(index=True, default=None)
    eni_id = StringProperty(default=None, index=True)
    instance_id = StringProperty(index=True, default=None)
    account_id = StringProperty(index=True, default=None)
    name = StringProperty(default=None)
    security_groups = ArrayProperty(index=True, default=None)
    tags = JSONProperty(default=None)

    simple_flows = RelationshipTo('Resource', 'SIMPLE_FLOW', model=SimpleFlowRel)
    flows = RelationshipTo('Resource', 'FLOW', model=FlowRel)
