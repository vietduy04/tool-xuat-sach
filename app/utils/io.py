import os


def save_file(file, persistent: bool = False) -> str:
    """Save file to temporary or persistent location."""
    if persistent:
        # Save to a persistent config directory
        os.makedirs(PERSISTENCE_STORE, exist_ok=True)
        file_path = os.path.join(PERSISTENCE_STORE, f"{file.name}")
    else:
        # Save to temporary location
        temp_dir = tempfile.mkdtemp()
        file_path = os.path.join(temp_dir, file.name)

    with open(file_path, "wb") as f:
        f.write(file.getbuffer())
    return file_path


def get_folder_child(directory, format) -> list[str]:
    results = []

    for name in os.listdir(directory):
        path = os.path.join(directory, name)

        if os.path.isfile(path) and name.lower().endswith(f".{format}"):
            results.append(name)

    return results
