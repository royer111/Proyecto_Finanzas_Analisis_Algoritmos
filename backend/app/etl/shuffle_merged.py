import csv
import random
from pathlib import Path


# ==========================================================
# ENCONTRAR RAIZ DEL PROYECTO AUTOMATICAMENTE
# ==========================================================

def find_project_root(start_path: Path) -> Path:
    """
    Sube directorios hasta encontrar la carpeta que contiene 'data'.
    """
    current = start_path

    while current != current.parent:
        if (current / "data").exists():
            return current
        current = current.parent

    raise FileNotFoundError("No se pudo encontrar la raíz del proyecto.")


# ==========================================================
# DESORDENAR CSV
# ==========================================================

def shuffle_merged_csv(file_path: Path, seed: int | None = 42):

    if not file_path.exists():
        print(f"[ERROR] Archivo no encontrado:\n{file_path}")
        return

    print(f"Leyendo archivo:\n{file_path}")

    with open(file_path, "r", newline="") as file:
        reader = csv.reader(file)
        header = next(reader)
        rows = list(reader)

    print(f"Filas cargadas: {len(rows)}")

    if seed is not None:
        random.seed(seed)

    # 🔥 desorden real
    random.shuffle(rows)

    with open(file_path, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)

    print("[OK] CSV desordenado correctamente.")


# ==========================================================
# MAIN
# ==========================================================

def main():

    # ruta del script actual
    script_path = Path(__file__).resolve()

    # encontrar raíz del proyecto automáticamente
    project_root = find_project_root(script_path.parent)

    merged_path = project_root / "data" / "merged" / "merged_desorganized.csv"

    shuffle_merged_csv(merged_path, seed=42)


if __name__ == "__main__":
    main()