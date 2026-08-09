import { useEffect, useMemo, useState } from "react";
import { Dumbbell } from "./charts/Dumbbell";
import { DivergingBar, HBar, StageBars } from "./charts/Bars";
import { Heatmap } from "./charts/Heatmap";
import { PairedTime } from "./charts/PairedTime";
import { Waterfall } from "./charts/Waterfall";
import { Table } from "./charts/chrome";
import {
  Grid,
  HeroFigure,
  Rail,
  Section,
  StatRow,
  StatTile,
  StatusPill,
  type NavItem,
  type Status,
} from "./components/ui";
import { count, inr, loadDataset, pct, shortMonth, type Dataset } from "./data";
import { pickActiveSection } from "./scrollspy";

const NAV: NavItem[] = [
  { id: "overview", label: "Overview", hint: "What the platform found" },
  { id: "clinical", label: "Clinical", hint: "Journeys and readmission" },
  { id: "operational", label: "Operational", hint: "Beds and attribution" },
  { id: "financial", label: "Financial", hint: "Claims and the gap" },
  { id: "quality", label: "Data quality", hint: "50 checks against truth" },
];

const WARD_ORDER = ["ICU", "HDU", "GENERAL", "SEMI_PRIVATE", "PRIVATE", "DELUXE"];

export default function App() {
  const [data, setData] = useState<Dataset | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [active, setActive] = useState("overview");
  const [theme, setTheme] = useState<"light" | "dark">("light");

  useEffect(() => {
    loadDataset().then(setData).catch((e: Error) => setError(e.message));
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
  }, [theme]);

  // Scroll spy for the rail.
  //
  // This was an IntersectionObserver with `threshold: [0.1, 0.5]` and a rootMargin
  // that narrowed the observation band to about a tenth of the viewport. That never
  // fired: thresholds are a fraction of *the target element*, and these sections run
  // to several thousand pixels, so a section can put at most ~3% of itself inside a
  // band that thin. The ratio never reached 0.1 and the callback never ran, leaving
  // the rail stuck on whatever loaded first.
  //
  // Reading positions directly sidesteps the whole problem, and is correct whatever
  // height a section happens to be. A rAF guard keeps it to one measurement per
  // frame, so it costs about as much as the observer would have.
  useEffect(() => {
    if (!data) return;

    let frame = 0;
    const update = () => {
      frame = 0;
      const tops = NAV.map((item) => {
        const el = document.getElementById(item.id);
        return { id: item.id, top: el ? el.getBoundingClientRect().top : Infinity };
      });
      const atBottom =
        window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 2;
      // The line the reader is actually reading at, a third of the way down.
      setActive(pickActiveSection(tops, window.innerHeight * 0.33, atBottom));
    };

    const onScroll = () => {
      if (!frame) frame = requestAnimationFrame(update);
    };

    update();
    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("resize", onScroll, { passive: true });
    return () => {
      window.removeEventListener("scroll", onScroll);
      window.removeEventListener("resize", onScroll);
      if (frame) cancelAnimationFrame(frame);
    };
  }, [data]);

  const occupancyCells = useMemo(
    () =>
      data?.operational.occupancy_grid.map((c) => ({
        row: c.hospital_name,
        col: c.ward_type,
        value: c.occupancy_pct,
        extra: [["Beds", count(c.beds)] as [string, string]],
      })) ?? [],
    [data],
  );

  if (error) {
    return (
      <main className="boot boot--error">
        <h1>Dashboard data not found</h1>
        <p>{error}</p>
        <pre>make web-data</pre>
      </main>
    );
  }
  if (!data) {
    return (
      <main className="boot">
        <div className="boot__pulse" aria-hidden="true" />
        <p>Reading the Gold layer…</p>
      </main>
    );
  }

  const { headline, clinical, operational, financial, quality, reference } = data;
  const c = headline.clinical;
  const f = headline.financial;
  const a = headline.attribution;
  const misattributedPct = (a.misattributed / a.total_attributed) * 100;

  return (
    <div className="shell">
      <Rail
        items={NAV}
        active={active}
        theme={theme}
        onTheme={() => setTheme((t) => (t === "light" ? "dark" : "light"))}
        generatedAt={headline.generated_at}
        environment={import.meta.env.MODE === "production" ? "batch export" : "batch export"}
      />

      <main className="main">
        {/* ------------------------------------------------------ overview */}
        <Section
          id="overview"
          eyebrow="MedChain Analytics"
          title="Three things the network could not see"
          lede={`Eight hospitals, five cities, ${count(reference.counts.visits)} visits over three years. Four source systems that share no keys. These are the findings that only exist once the data is joined — each one is a number the hospitals could not previously produce.`}
        >
          <div className="heroes">
            <HeroFigure
              value={`+${c.readmission_gap_pp.toFixed(2)}`}
              unit="pp"
              claim="Readmission is understated by every hospital"
              detail={`Each site sees ${pct(c.rate_hospital * 100, 2)}. Measured across the network through resolved patient identities it is ${pct(c.rate_network * 100, 2)} — ${count(c.hidden_readmissions)} readmissions where the patient returned somewhere else under a different patient_id.`}
              href="#clinical"
            />
            <HeroFigure
              value={count(a.misattributed)}
              claim="Consultations credited to the wrong department"
              detail={`${pct(misattributedPct)} of all visits. Without effective-dated doctor history, every past consultation is attributed to the department that doctor sits in today rather than the one they worked in at the time.`}
              href="#operational"
              tone="warn"
            />
            <HeroFigure
              value={inr(f.room_excess)}
              claim="Recoverable, out of a much larger gap"
              detail={`${inr(f.gap)} of ${inr(f.billed)} billed is not reimbursed. Most is contractual — ${inr(f.copay)} co-pay, ${inr(f.excluded)} exclusions. Only room-rent excess is leakage the hospital controls.`}
              href="#financial"
              tone="neutral"
            />
          </div>

          <StatRow>
            <StatTile label="Visits" value={count(reference.counts.visits)} sub="3 years, 8 hospitals" />
            <StatTile label="Patients resolved" value={count(reference.counts.patients)} sub="from 219,600 registrations" />
            <StatTile label="Claims" value={count(reference.counts.claims)} sub={`${count(reference.counts.claim_transitions)} state transitions rebuilt`} />
            <StatTile label="Quality checks" value={`${quality.summary.passed}/${quality.summary.total}`} sub={`${quality.summary.blocking_failures} blocking failures`} emphasis={quality.summary.blocking_failures === 0} />
          </StatRow>
        </Section>

        {/* ------------------------------------------------------ clinical */}
        <Section
          id="clinical"
          eyebrow="Clinical"
          title="Patient journeys across the network"
          lede="A patient registered at three hospitals was three unrelated records. The Master Patient Index resolves them into one person, which is what makes a network-wide readmission rate computable at all."
        >
          <StatRow>
            <StatTile label="Inpatient discharges" value={count(c.inpatient_visits)} />
            <StatTile label="Readmission — network" value={pct(c.rate_network * 100, 2)} sub="via resolved identity" emphasis />
            <StatTile label="Readmission — single hospital" value={pct(c.rate_hospital * 100, 2)} sub="what each site can see" />
            <StatTile label="Invisible readmissions" value={count(c.hidden_readmissions)} sub="returned to another hospital" />
          </StatRow>

          <Dumbbell
            title="Every hospital understates its own readmission rate"
            subtitle="30-day readmission, measured two ways. The bar between the dots is what the hospital cannot see."
            note="Ordered by the size of the gap. A patient discharged from Gachibowli and readmitted at Secunderabad is invisible to both sites individually — they hold different patient_id values for the same person."
            rows={clinical.readmission_by_hospital.map((r) => ({
              label: r.hospital_name.replace("MedChain ", ""),
              from: r.rate_hospital,
              to: r.rate_network,
              extra: [
                ["Discharges", count(r.discharges)],
                ["Hidden readmissions", count(r.hidden)],
                ["City", r.city],
              ],
            }))}
            fromLabel="Single hospital"
            toLabel="Network-wide"
            format={(v) => `${v.toFixed(2)}%`}
          />

          <Grid>
            <PairedTime
              title="Admissions and readmission rate over time"
              subtitle="Two panels, one shared time axis"
              note="Deliberately not a dual-axis chart. Two y-scales on one plot make the crossing point an artefact of the scales chosen rather than anything in the data."
              points={clinical.monthly.map((m) => ({ t: m.month, a: m.inpatient, b: m.readmission_pct }))}
              aLabel="Inpatient admissions"
              bLabel="Readmission rate"
              formatA={(v) => count(Math.round(v))}
              formatB={(v) => `${v.toFixed(1)}%`}
              formatT={shortMonth}
            />
            <div className="figure">
              <div className="figure__head">
                <div>
                  <h3 className="figure__title">Patients registered at more than one hospital</h3>
                  <p className="figure__sub">Each of these was previously several unrelated records</p>
                </div>
              </div>
              <div className="figure__body">
                <StatRow>
                  {clinical.registration_spread.map((r) => (
                    <StatTile
                      key={r.hospitals}
                      label={r.hospitals === 1 ? "One hospital" : `${r.hospitals} hospitals`}
                      value={count(r.patients)}
                      sub={r.hospitals > 1 ? "needed identity resolution" : "single registration"}
                      emphasis={r.hospitals > 1}
                    />
                  ))}
                </StatRow>
              </div>
              <figcaption className="figure__note">
                A count, not a chart. Three ordered magnitudes spanning two orders of
                magnitude are read more accurately as numbers than as bars — and a log
                axis would break the length encoding a bar depends on.
              </figcaption>
            </div>
          </Grid>

          <div className="figure">
            <div className="figure__head">
              <div>
                <h3 className="figure__title">Most common inpatient procedures</h3>
                <p className="figure__sub">Length of stay and readmission rate by procedure</p>
              </div>
            </div>
            <div className="figure__body">
              <Table
                columns={["Procedure", "Specialty", "Episodes", "Avg LOS", "Readmission"]}
                align={["l", "l", "r", "r", "r"]}
                rows={clinical.top_procedures.map((p) => [
                  p.procedure_name,
                  p.specialty,
                  count(p.episodes),
                  p.avg_los.toFixed(1),
                  pct(p.readmission_pct),
                ])}
              />
            </div>
          </div>
        </Section>

        {/* --------------------------------------------------- operational */}
        <Section
          id="operational"
          eyebrow="Operational"
          title="Beds, and who did the work"
          lede="Ward occupancy has to be reconstructed — the source logs movement events, never daily state. Department attribution has to be reconstructed too, because the HR roster keeps no history."
        >
          <Heatmap
            title="Mean occupancy by hospital and ward type"
            subtitle="Reconstructed from check-in, transfer and check-out events"
            note="One sequential hue rather than a red-to-green scale: occupancy is a magnitude, not a polarity, and a single ramp survives greyscale printing and every form of colour vision deficiency. Every cell carries its value, so the colour is reinforcement rather than the only encoding."
            cells={occupancyCells}
            colOrder={WARD_ORDER}
            format={(v) => `${v.toFixed(0)}%`}
          />

          <DivergingBar
            title="Consultations credited to the wrong department"
            subtitle="Point-in-time attribution versus the doctor's current department"
            note={`${count(a.misattributed)} consultations — ${pct(misattributedPct)} of all visits — would land in the wrong department without SCD Type 2 history. Departments on the right absorb work they never did; those on the left lose credit for work they performed.`}
            rows={operational.attribution_by_department
              .filter((d) => d.misattributed !== 0)
              .map((d) => ({
                label: d.department,
                value: d.misattributed,
                extra: [
                  ["Correct (point-in-time)", count(d.correct)],
                  ["Naive (current dept)", count(d.naive)],
                ],
              }))}
            negLabel="Loses credit"
            posLabel="Absorbs others' work"
            format={(v) => (v > 0 ? `+${count(v)}` : count(v))}
          />

          <Grid>
            <HBar
              title="Consultations per doctor"
              subtitle="Attributed by department at the time of the visit"
              valueLabel="Per doctor"
              rows={operational.doctor_utilisation.slice(0, 10).map((d) => ({
                label: d.department,
                value: d.per_doctor,
                extra: [
                  ["Consultations", count(d.consultations)],
                  ["Doctors", count(d.doctors)],
                ],
              }))}
              format={(v) => v.toFixed(0)}
            />
            <div className="figure">
              <div className="figure__head">
                <div>
                  <h3 className="figure__title">Wards under sustained pressure</h3>
                  <p className="figure__sub">More than 20 days above 85% occupancy</p>
                </div>
              </div>
              <div className="figure__body">
                <Table
                  columns={["Hospital", "Ward", "Beds", "Days &gt;85%", "Avg occ."]}
                  align={["l", "l", "r", "r", "r"]}
                  rows={operational.pressure_wards.slice(0, 10).map((w) => [
                    w.hospital_name.replace("MedChain ", ""),
                    w.ward_type.replace("_", " "),
                    count(w.beds),
                    count(w.days_above_85),
                    pct(w.avg_occupancy_pct, 0),
                  ])}
                />
              </div>
              <figcaption className="figure__note">
                Sustained high occupancy with a longer-than-median stay points at
                discharge process; with a normal stay it is genuine capacity pressure.
              </figcaption>
            </div>
          </Grid>
        </Section>

        {/* ---------------------------------------------------- financial */}
        <Section
          id="financial"
          eyebrow="Financial"
          title="Where the money goes"
          lede="The insurer reports one approved amount and no working. The TPA rules engine reconstructs the deduction cascade — exclusions, room-rent cap, co-pay, residual — and reconciles it against what was actually paid."
        >
          <StatRow>
            <StatTile label="Billed" value={inr(f.billed)} sub={`${count(f.claims)} claims`} />
            <StatTile label="Reimbursed" value={inr(f.reimbursed)} />
            <StatTile label="Gap" value={inr(f.gap)} sub={pct((f.gap / f.billed) * 100) + " of billed"} />
            <StatTile label="Recoverable" value={inr(f.room_excess)} sub="room rent above policy cap" emphasis />
          </StatRow>

          <Waterfall
            title="Billed to net reimbursed"
            subtitle="Every deduction, in the order a TPA applies them"
            note="Only room-rent excess is coloured as recoverable. It disappears if admission room category matches policy entitlement. Co-pay and contractual exclusions are the patient's and the policy's share — colouring all five deductions alike would imply money was being left on the table when it is not."
            steps={financial.waterfall}
            format={(v) => inr(Math.abs(v))}
          />

          <Grid>
            <StageBars
              title="Claim lifecycle"
              subtitle="Claims reaching each state, and how long they sit there"
              note="Not a funnel: claims branch to Approved or Rejected rather than draining through a pipe. The useful question is where they stall, so dwell time is shown as its own track."
              rows={financial.lifecycle_stages.map((s) => ({
                stage: s.status_code,
                claims: s.claims,
                dwellDays: s.avg_days_in_prev,
              }))}
            />
            <HBar
              title="Denial reasons by value"
              subtitle="Rejected claims, ranked by rupees at stake"
              valueLabel="Value"
              note="A reason appearing across six or more hospitals is a process defect worth fixing centrally; one confined to a single site is local training."
              rows={financial.denial_reasons.map((d) => ({
                label: d.rejection_reason,
                value: d.value,
                extra: [
                  ["Claims", count(d.claims)],
                  ["Hospitals affected", count(d.hospitals_affected)],
                ],
              }))}
              format={(v) => inr(v)}
              emphasise={(r) =>
                (financial.denial_reasons.find((d) => d.rejection_reason === r.label)
                  ?.hospitals_affected ?? 0) >= 6
              }
            />
          </Grid>

          <div className="figure">
            <div className="figure__head">
              <div>
                <h3 className="figure__title">Reimbursement gap by hospital and insurer</h3>
                <p className="figure__sub">Recoverable column is room-rent excess only</p>
              </div>
            </div>
            <div className="figure__body">
              <Table
                columns={["Hospital", "Insurer", "Billed", "Reimbursed", "Gap", "Gap %", "Recoverable"]}
                align={["l", "l", "r", "r", "r", "r", "r"]}
                rows={financial.gap_by_hospital.map((g) => [
                  g.hospital_name.replace("MedChain ", ""),
                  g.insurer_name,
                  inr(g.billed),
                  inr(g.reimbursed),
                  inr(g.gap),
                  pct(g.gap_pct),
                  inr(g.recoverable),
                ])}
              />
            </div>
          </div>
        </Section>

        {/* ------------------------------------------------------ quality */}
        <QualitySection quality={quality} reference={reference} />
      </main>
    </div>
  );
}

/* ---------------------------------------------------------------- quality */

function statusOf(check: { passed: boolean; severity: string }): Status {
  if (check.passed) return "good";
  return check.severity === "blocking" ? "critical" : "warning";
}

function QualitySection({
  quality,
  reference,
}: {
  quality: Dataset["quality"];
  reference: Dataset["reference"];
}) {
  const recovery = quality.checks.filter((c) => c.check_type === "recovery");
  const structural = quality.checks.filter((c) => c.check_type !== "recovery");

  return (
    <Section
      id="quality"
      eyebrow="Data quality"
      title="Measured, not asserted"
      lede="Two different kinds of check, kept visibly apart. Structural checks prove the warehouse is internally consistent — they would all pass even if every matching decision were wrong. Recovery metrics compare the output against ground truth and answer the question that matters: how much of what was destroyed did we get back?"
    >
      <StatRow>
        <StatTile label="Checks" value={String(quality.summary.total)} />
        <StatTile label="Passed" value={String(quality.summary.passed)} emphasis />
        <StatTile label="Blocking failures" value={String(quality.summary.blocking_failures)} sub="run fails if above zero" />
        <StatTile label="Warnings" value={String(quality.summary.warnings)} />
      </StatRow>

      <div className="figure">
        <div className="figure__head">
          <div>
            <h3 className="figure__title">Recovery metrics</h3>
            <p className="figure__sub">Measured against ground truth in <code>data/_truth</code></p>
          </div>
        </div>
        <div className="figure__body">
          <div className="checks">
            {recovery.map((c) => (
              <div className="check" key={c.check_name}>
                <StatusPill status={statusOf(c)}>{c.passed ? "Pass" : "Warn"}</StatusPill>
                <div className="check__body">
                  <div className="check__name">{c.check_name}</div>
                  <div className="check__detail">{c.detail}</div>
                </div>
                <div className="check__value num">
                  {c.actual_value === null ? "—" : c.actual_value.toFixed(4)}
                  {c.threshold !== null && (
                    <span className="check__threshold"> / {c.threshold.toFixed(2)}</span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
        <figcaption className="figure__note">
          The MPI numbers are pairwise precision and recall against known identities;
          claim reconstruction is measured against the true transition log; TPA accuracy
          is checked per component, because a net figure that lands close while the
          components are individually wrong is a coincidence, not a working rules engine.
        </figcaption>
      </div>

      <div className="figure">
        <div className="figure__head">
          <div>
            <h3 className="figure__title">Structural checks</h3>
            <p className="figure__sub">Keys, referential integrity, grains, ranges</p>
          </div>
        </div>
        <div className="figure__body">
          <Table
            columns={["Check", "Table", "Severity", "Value", "Result"]}
            align={["l", "l", "l", "r", "l"]}
            rows={structural.map((c) => [
              c.check_name,
              c.table_name,
              c.severity,
              c.actual_value === null ? "—" : c.actual_value.toFixed(4),
              c.passed ? "Pass" : c.severity === "blocking" ? "FAIL" : "Warn",
            ])}
          />
        </div>
      </div>

      <div className="figure">
        <div className="figure__head">
          <div>
            <h3 className="figure__title">ICD-10 code provenance</h3>
            <p className="figure__sub">How each code was obtained, not just that one exists</p>
          </div>
        </div>
        <div className="figure__body">
          <StatRow>
            {reference.icd_provenance.map((p) => (
              <StatTile
                key={p.icd10_source}
                label={p.icd10_source.replace("_", " ").toLowerCase()}
                value={count(p.procedures)}
                emphasis={p.icd10_source === "SOURCE"}
              />
            ))}
          </StatRow>
        </div>
        <figcaption className="figure__note">
          A single "100% filled" figure would be misleading: a specialty-level default is
          not the same fact as a coded diagnosis. The tier travels into the warehouse so
          an analyst filtering on a diagnosis can see which they are looking at.
        </figcaption>
      </div>
    </Section>
  );
}
