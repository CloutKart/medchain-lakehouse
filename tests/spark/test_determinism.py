"""Window functions must give the same answer regardless of how Spark splits the work.

A window ordered on a non-unique column leaves ``lag``/``row_number`` free to pick
either of two tied rows, and which one it picks depends on partitioning — so the
result changes with executor count. It is not caught by re-running in one
environment, which is why the existing idempotency test missed it.

It was found by comparing the same Gold build across two environments: local Spark
reported 7,474 hidden readmissions, Databricks reported 7,483. Nine clinical events
that existed or did not depending on cluster shape. The cause was
``orderBy("admission_date")`` over a dataset where 1,679 patients have two or more
visits sharing an admission date.

These tests repartition the same rows several ways and assert the output is
identical, which is the property that actually matters.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F
from pyspark.sql.window import Window

pytestmark = pytest.mark.spark


@pytest.fixture
def tied_visits(spark):
    """Visits deliberately full of same-day ties for one patient."""
    rows = [
        # (visit_id, mpi_id, hospital_id, admission_date, discharge_date, inpatient)
        ("V001", "MPI1", "H1", "2024-01-01", "2024-01-05", True),
        ("V002", "MPI1", "H2", "2024-01-20", "2024-01-25", True),  # readmit, other site
        ("V003", "MPI1", "H1", "2024-01-20", "2024-01-22", True),  # tie on the same date
        ("V004", "MPI1", "H3", "2024-02-01", "2024-02-03", True),
        ("V005", "MPI2", "H1", "2024-03-01", "2024-03-02", True),
        ("V006", "MPI2", "H1", "2024-03-01", "2024-03-04", True),  # tie
        ("V007", "MPI2", "H2", "2024-03-10", "2024-03-12", True),
    ]
    return (
        spark.createDataFrame(
            rows,
            "visit_id string, mpi_id string, hospital_id string, "
            "admission_date string, discharge_date string, is_inpatient boolean",
        )
        .withColumn("admission_date", F.to_date("admission_date"))
        .withColumn("discharge_date", F.to_date("discharge_date"))
    )


def readmission_flags(df, *, tie_break: bool):
    """The production window, with and without its tie-break, for comparison."""
    order = ["admission_date", "visit_id"] if tie_break else ["admission_date"]
    net = Window.partitionBy("mpi_id").orderBy(*order)
    return (
        df.withColumn("prev_discharge", F.lag("discharge_date").over(net))
        .withColumn("gap", F.datediff(F.col("admission_date"), F.col("prev_discharge")))
        .withColumn(
            "readmit",
            F.col("is_inpatient") & (F.col("gap") >= 0) & (F.col("gap") <= 30),
        )
        .select("visit_id", "readmit", "gap")
    )


class TestReadmissionDeterminism:
    def test_identical_across_partitionings(self, tied_visits):
        """The property that broke: same rows, different splits, same answer."""
        results = []
        for partitions in (1, 3, 7):
            flagged = readmission_flags(
                tied_visits.repartition(partitions, "mpi_id"), tie_break=True
            )
            results.append(
                sorted((r["visit_id"], r["readmit"], r["gap"]) for r in flagged.collect())
            )

        assert results[0] == results[1] == results[2], (
            "readmission flags changed with partition count — the window ordering "
            "is not a total order"
        )

    def test_identical_across_repeated_runs(self, tied_visits):
        first = sorted(
            (r["visit_id"], r["readmit"])
            for r in readmission_flags(tied_visits, tie_break=True).collect()
        )
        for _ in range(3):
            again = sorted(
                (r["visit_id"], r["readmit"])
                for r in readmission_flags(tied_visits, tie_break=True).collect()
            )
            assert again == first

    def test_tie_break_picks_the_earlier_visit_id(self, tied_visits):
        """Not just stable — stable on a rule someone can reason about.

        V002 and V003 share 2024-01-20. With visit_id as the tie-break V002 sorts
        first, taking its previous discharge from V001, and V003 then follows V002.
        """
        rows = {
            r["visit_id"]: (r["gap"], r["readmit"])
            for r in readmission_flags(tied_visits, tie_break=True).collect()
        }
        assert rows["V002"][0] == 15  # 2024-01-20 minus V001's discharge 2024-01-05
        assert rows["V002"][1] is True  # inside the 30-day window

        # V003 is admitted before V002 is discharged, so the gap is negative. The
        # production predicate requires gap >= 0, which is what stops an overlapping
        # or concurrent stay being counted as a readmission of itself.
        assert rows["V003"][0] == -5
        assert rows["V003"][1] is False
