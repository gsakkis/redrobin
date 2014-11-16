import numbers


def validate_throttle(throttle):
    if not (isinstance(throttle, numbers.Number) and throttle >= 0):
        raise ValueError("throttle must be a positive number ({!r} given)"
                         .format(throttle))
