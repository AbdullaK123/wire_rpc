

class InvalidFrameSizeError(Exception):
    def __init__(self, max_frame_size: int):
        super().__init__(
            f"Invalid frame size. Must be less than max frame size of {max_frame_size} bytes"
        )