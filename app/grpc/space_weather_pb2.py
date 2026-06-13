# Generated-style protobuf module for contracts/space_weather.proto.
# Source proto: contracts/space_weather.proto

from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

_sym_db = _symbol_database.Default()


def _field(name, number, field_type, label=1, type_name=None):
    field = _descriptor_pb2.FieldDescriptorProto()
    field.name = name
    field.number = number
    field.label = label
    field.type = field_type
    if type_name:
        field.type_name = type_name
    return field


def _message_type(name, fields):
    message = _descriptor_pb2.DescriptorProto()
    message.name = name
    message.field.extend(fields)
    return message


def _method(name, input_type, output_type):
    method = _descriptor_pb2.MethodDescriptorProto()
    method.name = name
    method.input_type = input_type
    method.output_type = output_type
    return method


file_proto = _descriptor_pb2.FileDescriptorProto()
file_proto.name = "space_weather.proto"
file_proto.package = "geostorm.spaceweather.v1"
file_proto.syntax = "proto3"

file_proto.message_type.extend(
    [
        _message_type(
            "GetContextRequest",
            [
                _field("start_date", 1, 9),
                _field("end_date", 2, 9),
            ],
        ),
        _message_type("NoaaSwpcAlertsRequest", []),
        _message_type(
            "NasaDonkiCmesRequest",
            [
                _field("start_date", 1, 9),
                _field("end_date", 2, 9),
            ],
        ),
        _message_type(
            "RawJsonResponse",
            [
                _field("source", 1, 9),
                _field("fetched_at", 2, 9),
                _field("raw_json", 3, 9),
                _field("errors", 4, 9, label=3),
            ],
        ),
        _message_type(
            "DateWindow",
            [
                _field("start_date", 1, 9),
                _field("end_date", 2, 9),
            ],
        ),
        _message_type(
            "SpaceWeatherContextResponse",
            [
                _field("source", 1, 9),
                _field("fetched_at", 2, 9),
                _field(
                    "date_window",
                    3,
                    11,
                    type_name=".geostorm.spaceweather.v1.DateWindow",
                ),
                _field("noaa_swpc_alerts_json", 4, 9),
                _field("nasa_donki_cmes_json", 5, 9),
                _field("risk_signals_json", 6, 9),
                _field("errors", 7, 9, label=3),
                _field("esa_source_status", 8, 9),
                _field("esa_data_json", 9, 9),
                _field("esa_dataset_id", 10, 9),
                _field("esa_error", 11, 9),
            ],
        ),
        _message_type("HealthRequest", []),
        _message_type("ReadyRequest", []),
        _message_type(
            "HealthResponse",
            [
                _field("status", 1, 9),
                _field("service", 2, 9),
                _field("message", 3, 9),
            ],
        ),
    ]
)

service = _descriptor_pb2.ServiceDescriptorProto()
service.name = "SpaceWeatherService"
service.method.extend(
    [
        _method(
            "GetContext",
            ".geostorm.spaceweather.v1.GetContextRequest",
            ".geostorm.spaceweather.v1.SpaceWeatherContextResponse",
        ),
        _method(
            "GetNoaaSwpcAlerts",
            ".geostorm.spaceweather.v1.NoaaSwpcAlertsRequest",
            ".geostorm.spaceweather.v1.RawJsonResponse",
        ),
        _method(
            "GetNasaDonkiCmes",
            ".geostorm.spaceweather.v1.NasaDonkiCmesRequest",
            ".geostorm.spaceweather.v1.RawJsonResponse",
        ),
        _method(
            "Health",
            ".geostorm.spaceweather.v1.HealthRequest",
            ".geostorm.spaceweather.v1.HealthResponse",
        ),
        _method(
            "Ready",
            ".geostorm.spaceweather.v1.ReadyRequest",
            ".geostorm.spaceweather.v1.HealthResponse",
        ),
    ]
)
file_proto.service.extend([service])

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(file_proto.SerializeToString())


def _make_message_class(proto_name, class_name):
    descriptor = DESCRIPTOR.message_types_by_name[proto_name]
    cls = _reflection.GeneratedProtocolMessageType(
        class_name,
        (_message.Message,),
        {
            "DESCRIPTOR": descriptor,
            "__module__": __name__,
        },
    )
    _sym_db.RegisterMessage(cls)
    return cls


GetContextRequest = _make_message_class("GetContextRequest", "GetContextRequest")
NoaaSwpcAlertsRequest = _make_message_class(
    "NoaaSwpcAlertsRequest", "NoaaSwpcAlertsRequest"
)
NasaDonkiCmesRequest = _make_message_class(
    "NasaDonkiCmesRequest", "NasaDonkiCmesRequest"
)
RawJsonResponse = _make_message_class("RawJsonResponse", "RawJsonResponse")
DateWindow = _make_message_class("DateWindow", "DateWindow")
SpaceWeatherContextResponse = _make_message_class(
    "SpaceWeatherContextResponse", "SpaceWeatherContextResponse"
)
HealthRequest = _make_message_class("HealthRequest", "HealthRequest")
ReadyRequest = _make_message_class("ReadyRequest", "ReadyRequest")
HealthResponse = _make_message_class("HealthResponse", "HealthResponse")
