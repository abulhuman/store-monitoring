from typing import List, Tuple, Literal
from datetime import datetime, time

StoreTimezone = Tuple[int, datetime]
StoreTimezoneRaw = Tuple[int, str]
StoreTimezoneList = List[StoreTimezone]
StoreTimezoneListRaw = List[StoreTimezoneRaw]

Status = Literal['active', 'inactive']
StoreStatus = Tuple[datetime, str]
StoreStatusList = List[StoreStatus]
StoreStatusRaw = Tuple[str, str]
StoreStatusListRaw = List[StoreStatusRaw]

StoreBusinessHours = Tuple[int, time, time]
StoreBusinessHoursList = List[StoreBusinessHours]
StoreBusinessHoursRaw = Tuple[int, str, str]
StoreBusinessHoursListRaw = List[StoreBusinessHoursRaw]
