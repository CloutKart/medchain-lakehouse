"""Spark tests for the two most subtle transformations: SCD2 and bed gap-fill.

These use hand-built fixtures of a dozen rows each. The point is not volume — it is
that every edge case is stated explicitly and named, so a regression tells you which
behaviour broke rather than that a row count changed.
"""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F

from medchain.silver.bed_gapfill import build_segments, expand_to_days
from medchain.silver.scd2 import apply_scd2, point_in_time_join, prepare_versions

pytestmark = pytest.mark.spark

HIGH_DATE = date(9999, 12, 31)


def roster(spark, rows):
    """Weekly HR roster snapshots: (doctor_id, department, hospital, effective_from)."""
    return spark.createDataFrame(
        rows, "doctor_id string, department string, hospital_id string, effective_from_src string"
    ).withColumn("effective_from_src", F.to_date("effective_from_src"))


class TestPrepareVersions:
    def test_repeated_snapshots_collapse_to_one_version(self, spark):
        # 5 identical weekly exports describe ONE assignment period, not five.
        # Without this collapse a 3-year history produces 156 versions per doctor.
        df = roster(
            spark,
            [
                ("D1", "Cardiology", "H001", "2024-01-01"),
                ("D1", "Cardiology", "H001", "2024-01-08"),
                ("D1", "Cardiology", "H001", "2024-01-15"),
                ("D1", "Cardiology", "H001", "2024-01-22"),
                ("D1", "Cardiology", "H001", "2024-01-29"),
            ],
        )
        versions = prepare_versions(
            df, ["doctor_id"], ["department", "hospital_id"], "effective_from_src"
        )
        assert versions.count() == 1

    def test_change_creates_a_new_version_and_closes_the_old(self, spark):
        df = roster(
            spark,
            [
                ("D1", "Cardiology", "H001", "2024-01-01"),
                ("D1", "Cardiology", "H001", "2024-02-01"),
                ("D1", "Emergency", "H001", "2024-03-01"),  # the rotation
                ("D1", "Emergency", "H001", "2024-04-01"),
            ],
        )
        versions = prepare_versions(
            df, ["doctor_id"], ["department", "hospital_id"], "effective_from_src"
        )
        rows = {r["department"]: r for r in versions.collect()}
        assert len(rows) == 2

        old, new = rows["Cardiology"], rows["Emergency"]
        assert old["effective_from"] == date(2024, 1, 1)
        # The old version closes the day BEFORE the new one starts, so the ranges
        # never overlap. An overlap makes point-in-time joins fan out silently.
        assert old["effective_to"] == date(2024, 2, 29)
        assert old["is_current"] is False
        assert new["effective_from"] == date(2024, 3, 1)
        assert new["effective_to"] == HIGH_DATE
        assert new["is_current"] is True

    def test_reverting_to_a_previous_department_is_a_new_version(self, spark):
        # A -> B -> A is three periods, not two. Deduplicating on the value rather
        # than on consecutive runs would lose the middle one.
        df = roster(
            spark,
            [
                ("D1", "Cardiology", "H001", "2024-01-01"),
                ("D1", "Emergency", "H001", "2024-02-01"),
                ("D1", "Cardiology", "H001", "2024-03-01"),
            ],
        )
        versions = prepare_versions(
            df, ["doctor_id"], ["department", "hospital_id"], "effective_from_src"
        )
        assert versions.count() == 3

    def test_exactly_one_open_version_per_doctor(self, spark):
        df = roster(
            spark,
            [
                ("D1", "Cardiology", "H001", "2024-01-01"),
                ("D1", "Emergency", "H001", "2024-03-01"),
                ("D2", "Neurology", "H002", "2024-01-01"),
            ],
        )
        versions = prepare_versions(
            df, ["doctor_id"], ["department", "hospital_id"], "effective_from_src"
        )
        open_versions = versions.filter(F.col("is_current")).groupBy("doctor_id").count().collect()
        assert all(r["count"] == 1 for r in open_versions)


class TestApplyScd2Idempotency:
    def test_rerunning_the_same_batch_changes_nothing(self, spark, tmp_delta_dir):
        """The single most important property of the whole pipeline."""
        df = roster(
            spark,
            [
                ("D1", "Cardiology", "H001", "2024-01-01"),
                ("D1", "Emergency", "H001", "2024-03-01"),
                ("D2", "Neurology", "H002", "2024-01-01"),
            ],
        )
        versions = prepare_versions(
            df, ["doctor_id"], ["department", "hospital_id"], "effective_from_src"
        )
        target = str(tmp_delta_dir / "dim_doctor")

        kwargs = dict(
            business_key=["doctor_id"],
            tracked_cols=["department", "hospital_id"],
            sk_name="doctor_sk",
        )
        apply_scd2(spark, versions, target, **kwargs)
        first = spark.read.format("delta").load(target)
        first_count, first_keys = first.count(), sorted(r["doctor_sk"] for r in first.collect())

        apply_scd2(spark, versions, target, **kwargs)  # replay
        second = spark.read.format("delta").load(target)

        assert second.count() == first_count
        # Surrogate keys must be stable too — regenerating them would orphan every
        # fact row that already references the old value.
        assert sorted(r["doctor_sk"] for r in second.collect()) == first_keys

    def test_new_version_is_appended_on_a_later_batch(self, spark, tmp_delta_dir):
        target = str(tmp_delta_dir / "dim_doctor")
        kwargs = dict(
            business_key=["doctor_id"],
            tracked_cols=["department", "hospital_id"],
            sk_name="doctor_sk",
        )

        batch1 = prepare_versions(
            roster(spark, [("D1", "Cardiology", "H001", "2024-01-01")]),
            ["doctor_id"],
            ["department", "hospital_id"],
            "effective_from_src",
        )
        apply_scd2(spark, batch1, target, **kwargs)
        assert spark.read.format("delta").load(target).count() == 1

        batch2 = prepare_versions(
            roster(
                spark,
                [
                    ("D1", "Cardiology", "H001", "2024-01-01"),
                    ("D1", "Emergency", "H001", "2024-06-01"),
                ],
            ),
            ["doctor_id"],
            ["department", "hospital_id"],
            "effective_from_src",
        )
        apply_scd2(spark, batch2, target, **kwargs)
        stored = spark.read.format("delta").load(target)
        assert stored.count() == 2
        assert stored.filter(F.col("is_current")).count() == 1


class TestPointInTimeJoin:
    def test_visit_resolves_to_the_department_of_its_time(self, spark, tmp_delta_dir):
        """The join that the entire SCD2 apparatus exists to enable."""
        target = str(tmp_delta_dir / "dim_doctor")
        versions = prepare_versions(
            roster(
                spark,
                [
                    ("D1", "Cardiology", "H001", "2024-01-01"),
                    ("D1", "Emergency", "H001", "2024-06-01"),
                ],
            ),
            ["doctor_id"],
            ["department", "hospital_id"],
            "effective_from_src",
        )
        apply_scd2(
            spark,
            versions,
            target,
            business_key=["doctor_id"],
            tracked_cols=["department", "hospital_id"],
            sk_name="doctor_sk",
        )
        dim = spark.read.format("delta").load(target)

        visits = spark.createDataFrame(
            [("V1", "D1", "2024-03-15"), ("V2", "D1", "2024-08-15")],
            "visit_id string, doctor_id string, admission_date string",
        ).withColumn("admission_date", F.to_date("admission_date"))

        joined = point_in_time_join(
            visits,
            dim,
            business_key=["doctor_id"],
            fact_date_col="admission_date",
            sk_name="doctor_sk",
            dim_cols=["department"],
        )
        result = {r["visit_id"]: r["department"] for r in joined.collect()}

        # March visit belongs to Cardiology even though the doctor is now in
        # Emergency. Joining on is_current would return Emergency for both.
        assert result == {"V1": "Cardiology", "V2": "Emergency"}

    def test_join_does_not_fan_out(self, spark, tmp_delta_dir):
        target = str(tmp_delta_dir / "dim_doctor")
        versions = prepare_versions(
            roster(
                spark,
                [
                    ("D1", "Cardiology", "H001", "2024-01-01"),
                    ("D1", "Emergency", "H001", "2024-06-01"),
                    ("D1", "Neurology", "H001", "2024-09-01"),
                ],
            ),
            ["doctor_id"],
            ["department", "hospital_id"],
            "effective_from_src",
        )
        apply_scd2(
            spark,
            versions,
            target,
            business_key=["doctor_id"],
            tracked_cols=["department", "hospital_id"],
            sk_name="doctor_sk",
        )
        dim = spark.read.format("delta").load(target)

        visits = spark.createDataFrame(
            [(f"V{i}", "D1", "2024-07-01") for i in range(10)],
            "visit_id string, doctor_id string, admission_date string",
        ).withColumn("admission_date", F.to_date("admission_date"))

        joined = point_in_time_join(
            visits,
            dim,
            business_key=["doctor_id"],
            fact_date_col="admission_date",
            sk_name="doctor_sk",
            dim_cols=["department"],
        )
        # Overlapping effective ranges would turn 10 visits into 20 or 30 rows and
        # silently inflate every downstream count.
        assert joined.count() == 10


def bed_events(spark, rows):
    return spark.createDataFrame(
        rows,
        "event_id string, visit_id string, patient_id string, hospital_id string, "
        "ward_id string, ward_type string, bed_number string, event_type string, event_ts string",
    )


class TestBedGapFill:
    AS_OF = date(2024, 3, 31)

    def test_simple_stay_expands_inclusively(self, spark):
        events = bed_events(
            spark,
            [
                ("E1", "V1", "P1", "H1", "W1", "GENERAL", "B01", "CHECK_IN", "2024-03-01 10:00:00"),
                ("E2", "V1", "P1", "H1", "W1", "GENERAL", None, "CHECK_OUT", "2024-03-04 12:00:00"),
            ],
        )
        days = expand_to_days(build_segments(events, self.AS_OF))
        # 1st through 4th inclusive = 4 occupied bed-days.
        assert days.count() == 4

    def test_same_day_admission_and_discharge_is_one_bed_day(self, spark):
        # An exclusive date range would drop this stay entirely, undercounting
        # day-case activity.
        events = bed_events(
            spark,
            [
                ("E1", "V1", "P1", "H1", "W1", "GENERAL", "B01", "CHECK_IN", "2024-03-01 08:00:00"),
                ("E2", "V1", "P1", "H1", "W1", "GENERAL", None, "CHECK_OUT", "2024-03-01 18:00:00"),
            ],
        )
        assert expand_to_days(build_segments(events, self.AS_OF)).count() == 1

    def test_mid_stay_transfer_splits_across_two_wards(self, spark):
        """The case that breaks naive first-to-last-event pairing."""
        events = bed_events(
            spark,
            [
                ("E1", "V1", "P1", "H1", "ICU", "ICU", "B01", "CHECK_IN", "2024-03-01 10:00:00"),
                ("E2", "V1", "P1", "H1", "ICU", "ICU", None, "TRANSFER_OUT", "2024-03-03 09:00:00"),
                (
                    "E3",
                    "V1",
                    "P1",
                    "H1",
                    "GEN",
                    "GENERAL",
                    "B12",
                    "TRANSFER_IN",
                    "2024-03-03 09:30:00",
                ),
                (
                    "E4",
                    "V1",
                    "P1",
                    "H1",
                    "GEN",
                    "GENERAL",
                    None,
                    "CHECK_OUT",
                    "2024-03-06 11:00:00",
                ),
            ],
        )
        days = expand_to_days(build_segments(events, self.AS_OF))
        per_ward = {
            r["ward_id"]: r["n"]
            for r in days.groupBy("ward_id").agg(F.count(F.lit(1)).alias("n")).collect()
        }
        # ICU 1st-3rd = 3 days; general 3rd-6th = 4 days. Pairing only the first and
        # last event would credit all 6 days to ICU and overstate critical care.
        assert per_ward == {"ICU": 3, "GEN": 4}

    def test_unclosed_stay_is_capped_and_flagged(self, spark):
        events = bed_events(
            spark,
            [
                ("E1", "V1", "P1", "H1", "W1", "GENERAL", "B01", "CHECK_IN", "2024-03-25 10:00:00"),
            ],
        )
        segments = build_segments(events, self.AS_OF)
        row = segments.collect()[0]
        assert row["is_open_stay"] is True
        # Capped at the batch date, not carried forward forever.
        assert row["end_date"] <= self.AS_OF

    def test_unclosed_stay_never_extends_past_the_cap(self, spark):
        # An admission from long ago with no discharge must not occupy a bed on
        # every day since — that would inflate occupancy for years.
        events = bed_events(
            spark,
            [
                ("E1", "V1", "P1", "H1", "W1", "GENERAL", "B01", "CHECK_IN", "2022-01-01 10:00:00"),
            ],
        )
        days = expand_to_days(build_segments(events, self.AS_OF))
        assert days.count() <= 46  # MAX_OPEN_STAY_DAYS + 1

    def test_concurrent_patients_counted_separately(self, spark):
        events = bed_events(
            spark,
            [
                ("E1", "V1", "P1", "H1", "W1", "GENERAL", "B01", "CHECK_IN", "2024-03-01 10:00:00"),
                ("E2", "V1", "P1", "H1", "W1", "GENERAL", None, "CHECK_OUT", "2024-03-03 10:00:00"),
                ("E3", "V2", "P2", "H1", "W1", "GENERAL", "B02", "CHECK_IN", "2024-03-02 10:00:00"),
                ("E4", "V2", "P2", "H1", "W1", "GENERAL", None, "CHECK_OUT", "2024-03-04 10:00:00"),
            ],
        )
        days = expand_to_days(build_segments(events, self.AS_OF))
        on_2nd = days.filter(F.col("occupancy_date") == F.lit(date(2024, 3, 2))).count()
        assert on_2nd == 2
