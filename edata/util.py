from datetime import date, datetime
from json.encoder import JSONEncoder


class DateTimeEncoder(JSONEncoder):
    #Override the default method
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.strftime ("%Y-%m-%dT%H:%M:%SZ")