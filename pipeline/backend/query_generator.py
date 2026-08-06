"""Generate search queries from city × business combinations."""


def generate_queries(cities: list[str], businesses: list[str]) -> list[str]:
    """Cross-product of cities and businesses."""
    queries = []
    for city in cities:
        for biz in businesses:
            queries.append(f"{biz} in {city}")
    return queries


# Default lists
DEFAULT_CITIES = [
    "Rajkot", "Ahmedabad", "Surat", "Vadodara", "Jamnagar",
    "Bhavnagar", "Junagadh", "Gandhinagar", "Anand", "Morbi"
]

DEFAULT_BUSINESSES = [
    "salon", "hotel", "restaurant", "gym", "hospital",
    "dentist", "school", "coaching class", "car repair", "plumber"
]
