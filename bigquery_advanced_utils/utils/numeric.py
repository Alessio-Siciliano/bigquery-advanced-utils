""" This module provides a set of useful functions for numeric variable. """


class Numeric:  # pylint: disable=too-few-public-methods
    """Main class for the Numeric helper."""

    def convert_bytes_to_unit(self, byte_count: int, unit: str) -> float:
        """Converts a number of bytes into a specified unit (KB, MB, GB, TB).

        Parameters
        ----------
        byte_count: int
            The number of bytes to convert.

        unit: str
            The target unit ("KB", "MB", "GB", "TB").

        Returns
        -------
        float
            The size converted to the specified unit.

        Raises
        ------
        ValueError
            if the value passed to the function are wrong
        """
        unit_multipliers = {
            "KB": 1024,
            "MB": 1024**2,
            "GB": 1024**3,
            "TB": 1024**4,
        }
        unit = unit.upper()
        if unit not in unit_multipliers:
            raise ValueError(
                f"Unsupported unit: {unit}.",
                "Choose from 'KB', 'MB', 'GB', 'TB'.",
            )

        return byte_count / unit_multipliers[unit]
