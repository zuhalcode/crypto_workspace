def generate_colors(n):
    colors = []
    for i in range(n):
        ratio = i / max(n - 1, 1)

        r = max(0, min(1, abs(ratio * 6 - 3) - max(abs(ratio * 6 - 4) - 1, 0)))
        g = max(0, min(1, 1 - abs(ratio * 6 - 2)))
        b = max(0, min(1, 1 - abs(ratio * 6 - 4)))

        colors.append((r, g, b))
    return colors


def print_dict_pretty(d, indent=0):
    for key, value in d.items():
        if isinstance(value, dict):
            print(" " * indent + f"{key}:")
            print_dict_pretty(value, indent + 4)
        else:
            print(" " * indent + f"{key:20}: {value:.2f}")
