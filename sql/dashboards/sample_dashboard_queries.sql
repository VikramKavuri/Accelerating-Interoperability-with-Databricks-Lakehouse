-- Top conditions by age group.
SELECT
  condition_display,
  age_group,
  patient_count
FROM gold_condition_prevalence
ORDER BY patient_count DESC, condition_display;

-- Encounter count by month.
SELECT
  encounter_month,
  encounter_count
FROM gold_encounter_utilization
ORDER BY encounter_month;

-- Claim cost by linked condition.
SELECT
  condition_display,
  claim_count,
  total_claim_amount,
  average_claim_amount
FROM gold_claim_cost_summary
ORDER BY total_claim_amount DESC;

-- Medication usage by condition.
SELECT
  medication_display,
  condition_display,
  request_count
FROM gold_medication_usage
ORDER BY request_count DESC, medication_display;

-- Diabetes and hypertension population-health cohort.
SELECT
  patient_id,
  age_group,
  gender,
  cohort_reason
FROM gold_population_health_cohort
ORDER BY age_group, patient_id;

-- Quality gates by latest batch.
SELECT
  check_name,
  table_name,
  status,
  failed_count,
  checked_at
FROM quality_check_results
ORDER BY status DESC, check_name;
