"""Simulated geography. Coordinates are public city centroids used purely to give
synthetic transactions a plausible location. They do NOT represent real customers."""
from __future__ import annotations

import math

# (city, ISO country, lat, lon)
CITIES: tuple[tuple[str, str, float, float], ...] = (
    ("London", "GB", 51.507, -0.128), ("Manchester", "GB", 53.481, -2.243),
    ("Paris", "FR", 48.857, 2.352), ("Lyon", "FR", 45.764, 4.836),
    ("Berlin", "DE", 52.520, 13.405), ("Munich", "DE", 48.135, 11.582),
    ("Madrid", "ES", 40.417, -3.704), ("Barcelona", "ES", 41.385, 2.173),
    ("Rome", "IT", 41.903, 12.496), ("Milan", "IT", 45.464, 9.190),
    ("Amsterdam", "NL", 52.368, 4.904), ("Dublin", "IE", 53.350, -6.260),
    ("Stockholm", "SE", 59.329, 18.069), ("Warsaw", "PL", 52.230, 21.012),
    ("New York", "US", 40.713, -74.006), ("Chicago", "US", 41.878, -87.630),
    ("San Francisco", "US", 37.775, -122.419), ("Austin", "US", 30.267, -97.743),
    ("Toronto", "CA", 43.653, -79.383), ("Mexico City", "MX", 19.433, -99.133),
    ("Sao Paulo", "BR", -23.551, -46.633), ("Buenos Aires", "AR", -34.604, -58.382),
    ("Lagos", "NG", 6.524, 3.379), ("Nairobi", "KE", -1.292, 36.822),
    ("Cairo", "EG", 30.044, 31.236), ("Johannesburg", "ZA", -26.204, 28.047),
    ("Dubai", "AE", 25.205, 55.271), ("Mumbai", "IN", 19.076, 72.878),
    ("Bengaluru", "IN", 12.972, 77.595), ("Delhi", "IN", 28.704, 77.102),
    ("Singapore", "SG", 1.352, 103.820), ("Bangkok", "TH", 13.756, 100.502),
    ("Hong Kong", "HK", 22.320, 114.169), ("Tokyo", "JP", 35.676, 139.650),
    ("Seoul", "KR", 37.567, 126.978), ("Sydney", "AU", -33.869, 151.209),
    ("Melbourne", "AU", -37.814, 144.963), ("Auckland", "NZ", -36.849, 174.763),
)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))
