# Generated-style gRPC client module for contracts/space_weather.proto.
# Source proto: contracts/space_weather.proto

import grpc

from app.grpc import space_weather_pb2 as space__weather__pb2


class SpaceWeatherServiceStub(object):
    def __init__(self, channel):
        self.GetContext = channel.unary_unary(
            "/geostorm.spaceweather.v1.SpaceWeatherService/GetContext",
            request_serializer=space__weather__pb2.GetContextRequest.SerializeToString,
            response_deserializer=space__weather__pb2.SpaceWeatherContextResponse.FromString,
        )
        self.GetNoaaSwpcAlerts = channel.unary_unary(
            "/geostorm.spaceweather.v1.SpaceWeatherService/GetNoaaSwpcAlerts",
            request_serializer=space__weather__pb2.NoaaSwpcAlertsRequest.SerializeToString,
            response_deserializer=space__weather__pb2.RawJsonResponse.FromString,
        )
        self.GetNasaDonkiCmes = channel.unary_unary(
            "/geostorm.spaceweather.v1.SpaceWeatherService/GetNasaDonkiCmes",
            request_serializer=space__weather__pb2.NasaDonkiCmesRequest.SerializeToString,
            response_deserializer=space__weather__pb2.RawJsonResponse.FromString,
        )
        self.Health = channel.unary_unary(
            "/geostorm.spaceweather.v1.SpaceWeatherService/Health",
            request_serializer=space__weather__pb2.HealthRequest.SerializeToString,
            response_deserializer=space__weather__pb2.HealthResponse.FromString,
        )
        self.Ready = channel.unary_unary(
            "/geostorm.spaceweather.v1.SpaceWeatherService/Ready",
            request_serializer=space__weather__pb2.ReadyRequest.SerializeToString,
            response_deserializer=space__weather__pb2.HealthResponse.FromString,
        )
