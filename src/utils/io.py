import os

def get_folder_child(directory, format) -> list[str]:
    results = []

    for name in os.listdir(directory):
        path = os.path.join(directory, name)

        if os.path.isfile(path) and name.lower().endswith(f".{format}"):
            results.append(name)

    return results
