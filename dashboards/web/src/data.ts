/* Typed loaders for the JSON that `medchain-web-export` writes out of the Gold layer.
 *
 * The shapes here mirror the queries in src/medchain/web/export.py one-for-one. A
 * pytest asserts the emitted numbers against the Gold tables, so if these types and
 * that module ever disagree the build fails rather than the dashboard quietly
 * rendering a stale or wrong figure.
 */

export interface Headline {
  clinical: {
    visits: number;
    patients: number;
    inpatient_visits: number;
    rate_network: number;
    rate_hospital: number;
    hidden_readmissions: number;
    readmission_gap_pp: number;
  };
  attribution: { misattributed: number; total_attributed: number };
  financial: {
    billed: number;
    reimbursed: number;
    gap: number;
    excluded: number;
    room_excess: number;
    copay: number;
    other_deduction: number;
    reconciled_rate: number;
    claims: number;
  };
  generated_at: string;
}

export interface ReadmissionRow {
  hospital_name: string;
  city: string;
  discharges: number;
  rate_hospital: number;
  rate_network: number;
  hidden: number;
}

export interface MonthlyRow {
  month: string;
  visits: number;
  inpatient: number;
  readmission_pct: number | null;
}

export interface Clinical {
  readmission_by_hospital: ReadmissionRow[];
  monthly: MonthlyRow[];
  registration_spread: { hospitals: number; patients: number }[];
  top_procedures: {
    procedure_name: string;
    specialty: string;
    episodes: number;
    avg_los: number;
    readmission_pct: number;
  }[];
}

export interface Operational {
  occupancy_grid: {
    hospital_name: string;
    ward_type: string;
    occupancy_pct: number;
    bed_days: number;
    beds: number;
  }[];
  occupancy_monthly: { month: string; ward_type: string; occupancy_pct: number }[];
  pressure_wards: {
    hospital_name: string;
    ward_id: string;
    ward_type: string;
    beds: number;
    avg_occupancy_pct: number;
    days_above_85: number;
    alos: number | null;
  }[];
  attribution_by_department: {
    department: string;
    correct: number;
    naive: number;
    misattributed: number;
  }[];
  doctor_utilisation: {
    department: string;
    consultations: number;
    doctors: number;
    per_doctor: number;
  }[];
}

export interface Financial {
  waterfall: { stage: string; amount: number; kind: string; recoverable: boolean }[];
  gap_by_hospital: {
    hospital_name: string;
    city: string;
    insurer_name: string;
    billed: number;
    reimbursed: number;
    gap: number;
    recoverable: number;
    gap_pct: number;
  }[];
  lifecycle_stages: { status_code: string; claims: number; avg_days_in_prev: number | null }[];
  dwell_by_stage: {
    insurer_name: string;
    stage: string;
    median_days: number;
    transitions: number;
  }[];
  denial_reasons: {
    rejection_reason: string;
    claims: number;
    value: number;
    hospitals_affected: number;
  }[];
  variance_classes: { variance_class: string; claims: number }[];
}

export interface QualityCheck {
  check_name: string;
  check_type: string;
  layer: string;
  table_name: string;
  severity: string;
  passed: boolean;
  actual_value: number | null;
  threshold: number | null;
  comparison: string | null;
  detail: string | null;
}

export interface Quality {
  checks: QualityCheck[];
  summary: {
    total: number;
    passed: number;
    blocking_failures: number;
    warnings: number;
    run_ts: string;
  };
}

export interface Reference {
  hospitals: {
    hospital_name: string;
    city: string;
    tier: string;
    bed_capacity: number;
    total_beds: number;
    size_band: string;
  }[];
  insurers: { insurer_name: string; tpa_name: string; scheme_type: string }[];
  icd_provenance: { icd10_source: string; procedures: number }[];
  counts: {
    visits: number;
    patients: number;
    claims: number;
    claim_transitions: number;
    ward_days: number;
    doctor_versions: number;
  };
}

export interface Dataset {
  headline: Headline;
  clinical: Clinical;
  operational: Operational;
  financial: Financial;
  quality: Quality;
  reference: Reference;
}

const PANELS = [
  "headline",
  "clinical",
  "operational",
  "financial",
  "quality",
  "reference",
] as const;

export async function loadDataset(): Promise<Dataset> {
  // `import.meta.env.BASE_URL` rather than a rooted path, so the site works from a
  // subdirectory (GitHub Pages project sites) without a rebuild.
  const base = import.meta.env.BASE_URL;
  const entries = await Promise.all(
    PANELS.map(async (panel) => {
      const response = await fetch(`${base}data/${panel}.json`);
      if (!response.ok) {
        throw new Error(
          `Could not load ${panel}.json (${response.status}). ` +
            `Run \`make web-data\` to export the Gold layer.`,
        );
      }
      return [panel, await response.json()] as const;
    }),
  );
  return Object.fromEntries(entries) as unknown as Dataset;
}

/* ------------------------------------------------------------- formatting */

const CRORE = 1e7;
const LAKH = 1e5;

/** Indian numbering: ₹ crore above 1 Cr, lakh below. Writing 3,527 Cr as 35.27bn
 *  would be arithmetically fine and read as foreign to the audience. */
export function inr(amount: number, digits = 0): string {
  if (Math.abs(amount) >= CRORE) return `₹${(amount / CRORE).toFixed(digits)} Cr`;
  if (Math.abs(amount) >= LAKH) return `₹${(amount / LAKH).toFixed(1)} L`;
  return `₹${Math.round(amount).toLocaleString("en-IN")}`;
}

export function count(value: number): string {
  return value.toLocaleString("en-IN");
}

export function pct(value: number, digits = 1): string {
  return `${value.toFixed(digits)}%`;
}

export function shortMonth(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleDateString("en-GB", { month: "short", year: "2-digit" });
}
