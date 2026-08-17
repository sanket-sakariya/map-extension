from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config import DATABASE_URL

# Defense-in-depth: cap how long ANY single statement can run on this connection pool.
# Individual endpoints may set a tighter SET LOCAL statement_timeout for specific queries;
# this is the outer safety net so no request can hang the API indefinitely.
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"options": "-c statement_timeout=15000"}  # 15s hard ceiling per statement
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
