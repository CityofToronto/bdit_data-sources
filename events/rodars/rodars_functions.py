import struct
from io import BytesIO

class Coordinates:
    def __init__(self, longitude, latitude):
        self.longitude = longitude
        self.latitude = latitude

    @classmethod
    def from_binary(cls, br):
        # Read longitude and latitude as doubles (8 bytes each)
        longitude, latitude = struct.unpack('dd', br.read(16))
        return cls(longitude, latitude)

def geometry_from_bytes(geo_bytes):
    # Initialize a stream to read binary data from the byte array
    with BytesIO(geo_bytes) as ms:
        # Read the first 4 bytes for the integer (len in C#)
        len_val = struct.unpack('i', ms.read(4))[0]
        coordinates_list = []
        
        # Iterate and unpack each pair of doubles as coordinates
        for _ in range(len_val):
            coordinates = Coordinates.from_binary(ms)
            coordinates_list.append(coordinates)
        
        return coordinates_list
