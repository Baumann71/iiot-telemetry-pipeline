create extension if not exists timescaledb;

create table if not exists lecturas_v1 (
    ts TIMESTAMPTZ NOT NULL, --ts del sensor
    pozo_id TEXT NOT NULL,
    sensor TEXT NOT NULL,
    valor DOUBLE PRECISION NOT NULL,
    calidad TEXT, -- buena/sospechosa/mala (consumidores/calidad.py)
    calidad_motivo TEXT, -- por que no es buena (NULL si es buena)
    kafka_part INT, --particion y offset del mensaje en kafka
    kafka_offset BIGINT,
    ingest_ts TIMESTAMPTZ NOT NULL DEFAULT now(),--para medir la latencia de ingestion
    PRIMARY KEY (pozo_id, ts, sensor)
);

select create_hypertable('lecturas_v1', by_range('ts', INTERVAL '1 day'), if_not_exists => TRUE);
--particionamos por ts cada 1 dia una particion



-- tabla del grupo v2 (replay desde el offset 0), misma estructura que v1
create table if not exists lecturas_v2 (like lecturas_v1 including all);
select create_hypertable('lecturas_v2', by_range('ts', INTERVAL '1 day'), if_not_exists => TRUE);
