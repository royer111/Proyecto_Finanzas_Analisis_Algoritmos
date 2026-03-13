from backend.app.model.time_series import TimeSeries


def main():
    ts = TimeSeries()

    # Cambia estos nombres por activos reales en tu merged_prices.csv
    asset_1 = "AAPL"
    asset_2 = "MSFT"

    # Probar precios
    dates, s1, s2 = ts.get_series(asset_1, asset_2, "prices")

    print("=== PRECIOS ===")
    print("Cantidad de registros:", len(dates))
    print("Primer precio AAPL:", s1[0])
    print("Primer precio MSFT:", s2[0])

    # Probar retornos
    dates_r, r1, r2 = ts.get_series(asset_1, asset_2, "returns")

    print("\n=== RETORNOS ===")
    print("Cantidad de retornos:", len(r1))
    print("Primer retorno AAPL:", r1[0])
    print("Primer retorno MSFT:", r2[0])


if __name__ == "__main__":
    main()