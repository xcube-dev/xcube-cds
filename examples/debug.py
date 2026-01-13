from xcube.core.store import new_data_store

store = new_data_store("cds")
print(store.list_data_ids())
ds = store.open_data(
    "reanalysis-era5-single-levels-timeseries",
    variable_names=[
        "2m_dewpoint_temperature",
        "2m_temperature",
        "mean_wave_direction",
        "mean_wave_period",
    ],
    location=(10, 53.5),
    time_range=("1940-01-01", "2026-01-07"),
)

print(ds)
