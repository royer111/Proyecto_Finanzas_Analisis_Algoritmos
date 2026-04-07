# ==========================================================
# MAIN
# ==========================================================
from pathlib import Path

from backend.app.etl.shuffle_merged import shuffle_merged_csv


def main():

    merged_path = Path("backend/data/merged/merged_desorganized.csv")

    shuffle_merged_csv(
        file_path=merged_path,
        seed=42  # cambia o pon None para aleatorio distinto cada vez
    )


if __name__ == "__main__":
    main()