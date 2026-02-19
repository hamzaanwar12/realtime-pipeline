"""DuckDB sink: idempotent micro-batch upserts + SQL models rebuilt from models/*.sql."""
from pathlib import Path
import duckdb

MODELS = Path(__file__).resolve().parents[2] / "models"


class Sink:
    def __init__(self, path: str = ":memory:"):
        self.con = duckdb.connect(path)
        self.con.execute("""
            create table if not exists raw_events(event_id varchar primary key, user_id varchar, type varchar,
                ts timestamp, amount double);
            create table if not exists dead_letter(received_at timestamp default now(), payload varchar);
        """)

    def write(self, events: list[dict], bad: list[bytes] = ()) -> int:
        """Insert a batch in one transaction; duplicate event_ids (Kafka redelivery) are ignored."""
        self.con.begin()
        if events:
            self.con.executemany(
                "insert or ignore into raw_events values (?, ?, ?, to_timestamp(?), ?)",
                [(e["event_id"], e["user_id"], e["type"], e["ts"], e["amount"]) for e in events])
        if bad:
            self.con.executemany("insert into dead_letter(payload) values (?)",
                                 [(b.decode(errors="replace"),) for b in bad])
        self.con.commit()
        return len(events)

    def build_models(self, folder: Path = MODELS) -> list[str]:
        """Run each models/NN_name.sql as `create or replace view name as ...`, in file order."""
        built = []
        for f in sorted(folder.glob("*.sql")):
            name = f.stem.split("_", 1)[1]
            self.con.execute(f"create or replace view {name} as {f.read_text()}")
            built.append(name)
        return built

    def query(self, sql: str) -> list[tuple]:
        return self.con.execute(sql).fetchall()
