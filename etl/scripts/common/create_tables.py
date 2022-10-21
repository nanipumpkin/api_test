from base import Base, engine

# Import the Job table
from tables import JobRawAll, PprCleanAll

# Create the table in the database
if __name__ == "__main__":
    Base.metadata.create_all(engine)