# Databricks notebook source
"""Publish Gold analytics tables for SQL dashboards."""

# COMMAND ----------

# MAGIC %run ./_shared

# COMMAND ----------

create_target_schema()

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {table_name('gold_patient_summary')} AS
    SELECT
      p.patient_id,
      CASE
        WHEN year(current_date()) - year(to_date(p.birth_date)) < 18 THEN '0-17'
        WHEN year(current_date()) - year(to_date(p.birth_date)) < 35 THEN '18-34'
        WHEN year(current_date()) - year(to_date(p.birth_date)) < 50 THEN '35-49'
        WHEN year(current_date()) - year(to_date(p.birth_date)) < 65 THEN '50-64'
        ELSE '65+'
      END AS age_group,
      p.gender,
      count(DISTINCT c.condition_id) AS condition_count,
      count(DISTINCT e.encounter_id) AS encounter_count,
      count(DISTINCT m.medication_request_id) AS medication_count,
      round(sum(CASE WHEN cl.amount >= 0 THEN cl.amount ELSE 0 END), 2) AS valid_claim_amount
    FROM {table_name('silver_patient')} p
    LEFT JOIN {table_name('silver_condition')} c ON p.patient_id = c.patient_id
    LEFT JOIN {table_name('silver_encounter')} e ON p.patient_id = e.patient_id
    LEFT JOIN {table_name('silver_medication')} m ON p.patient_id = m.patient_id
    LEFT JOIN {table_name('silver_claim')} cl ON p.patient_id = cl.patient_id
    GROUP BY p.patient_id, p.birth_date, p.gender
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {table_name('gold_condition_prevalence')} AS
    SELECT
      c.condition_display,
      s.age_group,
      count(DISTINCT c.patient_id) AS patient_count
    FROM {table_name('silver_condition')} c
    JOIN {table_name('gold_patient_summary')} s ON c.patient_id = s.patient_id
    GROUP BY c.condition_display, s.age_group
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {table_name('gold_encounter_utilization')} AS
    SELECT
      date_format(to_timestamp(start_datetime), 'yyyy-MM') AS encounter_month,
      count(*) AS encounter_count
    FROM {table_name('silver_encounter')}
    GROUP BY date_format(to_timestamp(start_datetime), 'yyyy-MM')
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {table_name('gold_claim_cost_summary')} AS
    SELECT
      coalesce(c.condition_display, 'no linked condition') AS condition_display,
      count(cl.claim_id) AS claim_count,
      round(sum(cl.amount), 2) AS total_claim_amount,
      round(avg(cl.amount), 2) AS average_claim_amount
    FROM {table_name('silver_claim')} cl
    LEFT JOIN {table_name('silver_condition')} c ON cl.patient_id = c.patient_id
    WHERE cl.amount >= 0
    GROUP BY coalesce(c.condition_display, 'no linked condition')
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {table_name('gold_medication_usage')} AS
    SELECT
      m.medication_display,
      coalesce(c.condition_display, 'no linked condition') AS condition_display,
      count(*) AS request_count
    FROM {table_name('silver_medication')} m
    LEFT JOIN {table_name('silver_condition')} c ON m.patient_id = c.patient_id
    GROUP BY m.medication_display, coalesce(c.condition_display, 'no linked condition')
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {table_name('gold_population_health_cohort')} AS
    SELECT DISTINCT
      p.patient_id,
      s.age_group,
      p.gender,
      concat_ws(', ', collect_set(lower(c.condition_display))) AS cohort_reason
    FROM {table_name('silver_patient')} p
    JOIN {table_name('silver_condition')} c ON p.patient_id = c.patient_id
    JOIN {table_name('gold_patient_summary')} s ON p.patient_id = s.patient_id
    WHERE lower(c.condition_display) LIKE '%diabetes%'
       OR lower(c.condition_display) LIKE '%hypertens%'
    GROUP BY p.patient_id, s.age_group, p.gender
    """
)
