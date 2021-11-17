# -*- coding: utf-8 -*-
"""Helper classes for shazbuckbot"""

from enum import IntEnum, auto
from discord.ext import commands


class GameStatus(IntEnum):
    PICKING = auto()
    CANCELLED = auto()
    INPROGRESS = auto()
    TEAM1 = auto()
    TEAM2 = auto()
    TIED = auto()


class WagerResult(IntEnum):
    INPROGRESS = auto()
    WON = auto()
    LOST = auto()
    CANCELLED = auto()
    CANCELLEDNOWINNERS = auto()
    CANCELLEDONESIDED = auto()


class TransferReason(IntEnum):
    GIFT = auto()
    PLACEBET = auto()
    CANCELBET = auto()
    WINBET = auto()
    REVERTWIN = auto()


class TimeDuration:
    SECONDS_PER_UNIT = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}

    def __init__(self, value: int, unit: str):
        if not isinstance(value, int):
            raise TypeError('value argument not an integer')
        if not isinstance(unit, str) or unit not in TimeDuration.SECONDS_PER_UNIT:
            raise TypeError('unit argument not an a unit letter')
        self.value = value
        self.unit = unit

    def __str__(self):
        return f'{str(self.value)}{self.unit}'

    @classmethod
    async def convert(cls, _ctx, argument):
        return cls.from_string(argument)

    @classmethod
    def from_string(cls, argument):
        if isinstance(argument, str) and len(argument) > 1:
            unit = argument[-1]
            if unit in TimeDuration.SECONDS_PER_UNIT and argument[:-1].isdigit():
                value = int(argument[:-1])
                return cls(value, unit)
            else:
                raise commands.BadArgument('Incorrect string format for TimeDuration.')
        else:
            raise commands.BadArgument('No string or too short for TimeDuration.')

    @classmethod
    def from_seconds(cls, seconds: int):
        times = {key: round(seconds/TimeDuration.SECONDS_PER_UNIT[key]) for key in TimeDuration.SECONDS_PER_UNIT}
        value = 0
        unit = 's'
        for key in times:
            if value == 0 or (times[key] != 0 and times[key] < value):
                value = times[key]
                unit = key
        return cls(value, unit)

    @property
    def to_seconds(self):
        return self.value * TimeDuration.SECONDS_PER_UNIT[self.unit]
