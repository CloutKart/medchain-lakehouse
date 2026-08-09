/* Bar family: horizontal magnitude, diverging, and ordered stage bars.
 *
 * Marks follow one spec throughout — 4px rounded ends on the data end only, square
 * against the baseline, so length stays readable from the axis. A bar rounded at both
 * ends detaches from its baseline and starts lying about magnitude.
 */

import { scaleBand, scaleLinear } from "d3-scale";
import { useState } from "react";
import { Baseline, Figure, M, Table, Tooltip, type TipState } from "./chrome";

/** A rect with only its data end rounded. */
function BarPath({
  x,
  y,
  width,
  height,
  r = 4,
  dir,
  fill,
}: {
  x: number;
  y: number;
  width: number;
  height: number;
  r?: number;
  dir: "right" | "left" | "up";
  fill: string;
}) {
  const rr = Math.max(0, Math.min(r, dir === "up" ? width / 2 : height / 2, dir === "up" ? height : width));
  let d: string;
  if (dir === "right") {
    d = `M${x},${y} H${x + width - rr} a${rr},${rr} 0 0 1 ${rr},${rr} V${y + height - rr} a${rr},${rr} 0 0 1 ${-rr},${rr} H${x} Z`;
  } else if (dir === "left") {
    d = `M${x + width},${y} H${x + rr} a${rr},${rr} 0 0 0 ${-rr},${rr} V${y + height - rr} a${rr},${rr} 0 0 0 ${rr},${rr} H${x + width} Z`;
  } else {
    d = `M${x},${y + height} V${y + rr} a${rr},${rr} 0 0 1 ${rr},${-rr} H${x + width - rr} a${rr},${rr} 0 0 1 ${rr},${rr} V${y + height} Z`;
  }
  return <path d={d} fill={fill} />;
}

/* ------------------------------------------------------- horizontal bars */

export interface HBarRow {
  label: string;
  value: number;
  extra?: [string, string][];
}

export function HBar({
  rows,
  title,
  subtitle,
  note,
  valueLabel,
  format,
  color = "var(--cat-1)",
  /** Emphasis form: highlight one bar, recede the rest. The most underused chart
   *  form and often the honest answer to "make this clearer". */
  emphasise,
  height,
}: {
  rows: HBarRow[];
  title: string;
  subtitle?: string;
  note?: string;
  valueLabel: string;
  format: (v: number) => string;
  color?: string;
  emphasise?: (row: HBarRow) => boolean;
  height?: number;
}) {
  const [tip, setTip] = useState<TipState | null>(null);
  const width = 720;
  const rowH = 26;
  const m = M(8, 96, 8, 200);
  const h = height ?? m.top + m.bottom + rows.length * rowH;
  const iw = width - m.left - m.right;
  const ih = h - m.top - m.bottom;

  const y = scaleBand<string>().domain(rows.map((r) => r.label)).range([0, ih]).padding(0.34);
  const x = scaleLinear()
    .domain([0, Math.max(...rows.map((r) => r.value)) || 1])
    .range([0, iw]);

  return (
    <Figure
      title={title}
      subtitle={subtitle}
      note={note}
      table={
        <Table
          columns={["", valueLabel]}
          align={["l", "r"]}
          rows={rows.map((r) => [r.label, format(r.value)])}
        />
      }
    >
      <div className="plot" onMouseLeave={() => setTip(null)}>
        <svg viewBox={`0 0 ${width} ${h}`} role="img" aria-label={title}>
          <g transform={`translate(${m.left},${m.top})`}>
            <Baseline y={ih} x0={0} x1={0} />
            {rows.map((r) => {
              const by = y(r.label) ?? 0;
              const bw = Math.max(1, x(r.value));
              const dim = emphasise ? !emphasise(r) : false;
              return (
                <g
                  key={r.label}
                  onMouseMove={(e) => {
                    const box = (e.currentTarget.ownerSVGElement as SVGSVGElement)
                      .parentElement!.getBoundingClientRect();
                    setTip({
                      x: e.clientX - box.left,
                      y: e.clientY - box.top,
                      title: r.label,
                      rows: [[valueLabel, format(r.value)], ...(r.extra ?? [])],
                    });
                  }}
                >
                  <rect x={-m.left} y={by - 3} width={width} height={y.bandwidth() + 6} fill="transparent" />
                  <BarPath
                    x={0}
                    y={by}
                    width={bw}
                    height={y.bandwidth()}
                    dir="right"
                    fill={dim ? "var(--recede)" : color}
                  />
                  <text
                    x={-12}
                    y={by + y.bandwidth() / 2}
                    dy="0.32em"
                    textAnchor="end"
                    fontSize={12}
                    fill={dim ? "var(--ink-muted)" : "var(--ink-secondary)"}
                  >
                    {r.label}
                  </text>
                  <text
                    x={bw + 10}
                    y={by + y.bandwidth() / 2}
                    dy="0.32em"
                    fontSize={11}
                    fontFamily="var(--font-mono)"
                    fill="var(--ink-secondary)"
                  >
                    {format(r.value)}
                  </text>
                </g>
              );
            })}
          </g>
        </svg>
        <Tooltip tip={tip} width={width} />
      </div>
    </Figure>
  );
}

/* ----------------------------------------------------------- diverging */

export function DivergingBar({
  rows,
  title,
  subtitle,
  note,
  negLabel,
  posLabel,
  format,
}: {
  rows: HBarRow[];
  title: string;
  subtitle?: string;
  note?: string;
  negLabel: string;
  posLabel: string;
  format: (v: number) => string;
}) {
  const [tip, setTip] = useState<TipState | null>(null);
  const width = 720;
  const rowH = 24;
  const m = M(10, 72, 10, 196);
  const h = m.top + m.bottom + rows.length * rowH;
  const iw = width - m.left - m.right;
  const ih = h - m.top - m.bottom;

  const y = scaleBand<string>().domain(rows.map((r) => r.label)).range([0, ih]).padding(0.32);
  const extent = Math.max(...rows.map((r) => Math.abs(r.value))) || 1;
  const x = scaleLinear().domain([-extent, extent]).range([0, iw]);
  const zero = x(0);

  return (
    <Figure
      title={title}
      subtitle={subtitle}
      note={note}
      table={
        <Table
          columns={["Department", "Difference"]}
          align={["l", "r"]}
          rows={rows.map((r) => [r.label, format(r.value)])}
        />
      }
    >
      <div className="plot" onMouseLeave={() => setTip(null)}>
        <svg viewBox={`0 0 ${width} ${h}`} role="img" aria-label={title}>
          <g transform={`translate(${m.left},${m.top})`}>
            {/* The zero rule is the reference the whole chart is read against. */}
            <line x1={zero} x2={zero} y1={-4} y2={ih + 4} stroke="var(--axis)" shapeRendering="crispEdges" />
            {rows.map((r) => {
              const by = y(r.label) ?? 0;
              const neg = r.value < 0;
              const bw = Math.max(1, Math.abs(x(r.value) - zero));
              return (
                <g
                  key={r.label}
                  onMouseMove={(e) => {
                    const box = (e.currentTarget.ownerSVGElement as SVGSVGElement)
                      .parentElement!.getBoundingClientRect();
                    setTip({
                      x: e.clientX - box.left,
                      y: e.clientY - box.top,
                      title: r.label,
                      rows: [
                        ["Difference", format(r.value)],
                        ["Direction", neg ? negLabel : posLabel],
                        ...(r.extra ?? []),
                      ],
                    });
                  }}
                >
                  <rect x={-m.left} y={by - 3} width={width} height={y.bandwidth() + 6} fill="transparent" />
                  <BarPath
                    x={neg ? zero - bw : zero}
                    y={by}
                    width={bw}
                    height={y.bandwidth()}
                    dir={neg ? "left" : "right"}
                    fill={neg ? "var(--div-neg)" : "var(--div-pos)"}
                  />
                  <text
                    x={-12}
                    y={by + y.bandwidth() / 2}
                    dy="0.32em"
                    textAnchor="end"
                    fontSize={12}
                    fill="var(--ink-secondary)"
                  >
                    {r.label}
                  </text>
                  <text
                    x={neg ? zero - bw - 8 : zero + bw + 8}
                    y={by + y.bandwidth() / 2}
                    dy="0.32em"
                    textAnchor={neg ? "end" : "start"}
                    fontSize={11}
                    fontFamily="var(--font-mono)"
                    fill="var(--ink-muted)"
                  >
                    {format(r.value)}
                  </text>
                </g>
              );
            })}
          </g>
        </svg>
        <Tooltip tip={tip} width={width} />
        <ul className="legend">
          <li>
            <span className="legend__swatch" style={{ background: "var(--div-neg)" }} aria-hidden="true" />
            {negLabel}
          </li>
          <li>
            <span className="legend__swatch" style={{ background: "var(--div-pos)" }} aria-hidden="true" />
            {posLabel}
          </li>
        </ul>
      </div>
    </Figure>
  );
}

/* --------------------------------------------------------- stage bars */

/** Ordered stages, replacing a funnel.
 *
 *  A funnel implies volume draining through a pipe, which is not what a claim
 *  lifecycle does — claims branch to Approved or Rejected, and the interesting
 *  question is where they *stall*, not how many leak. So: magnitude per stage, with
 *  dwell time shown alongside as a second, separate encoding.
 */
export function StageBars({
  rows,
  title,
  subtitle,
  note,
}: {
  rows: { stage: string; claims: number; dwellDays: number | null }[];
  title: string;
  subtitle?: string;
  note?: string;
}) {
  const [tip, setTip] = useState<TipState | null>(null);
  const width = 720;
  const m = M(14, 150, 10, 150);
  const rowH = 40;
  const h = m.top + m.bottom + rows.length * rowH;
  const iw = width - m.left - m.right;
  const ih = h - m.top - m.bottom;

  const y = scaleBand<string>().domain(rows.map((r) => r.stage)).range([0, ih]).padding(0.4);
  const x = scaleLinear().domain([0, Math.max(...rows.map((r) => r.claims)) || 1]).range([0, iw]);
  const maxDwell = Math.max(...rows.map((r) => r.dwellDays ?? 0)) || 1;

  return (
    <Figure
      title={title}
      subtitle={subtitle}
      note={note}
      table={
        <Table
          columns={["Stage", "Claims", "Median days in stage"]}
          align={["l", "r", "r"]}
          rows={rows.map((r) => [
            r.stage,
            r.claims.toLocaleString("en-IN"),
            r.dwellDays === null ? "—" : r.dwellDays.toFixed(1),
          ])}
        />
      }
    >
      <div className="plot" onMouseLeave={() => setTip(null)}>
        <svg viewBox={`0 0 ${width} ${h}`} role="img" aria-label={title}>
          <g transform={`translate(${m.left},${m.top})`}>
            {rows.map((r) => {
              const by = y(r.stage) ?? 0;
              const bw = Math.max(2, x(r.claims));
              const dwellW = ((r.dwellDays ?? 0) / maxDwell) * 96;
              return (
                <g
                  key={r.stage}
                  onMouseMove={(e) => {
                    const box = (e.currentTarget.ownerSVGElement as SVGSVGElement)
                      .parentElement!.getBoundingClientRect();
                    setTip({
                      x: e.clientX - box.left,
                      y: e.clientY - box.top,
                      title: r.stage,
                      rows: [
                        ["Claims", r.claims.toLocaleString("en-IN")],
                        ["Median days in stage", r.dwellDays === null ? "—" : r.dwellDays.toFixed(1)],
                      ],
                    });
                  }}
                >
                  <rect x={-m.left} y={by - 6} width={width} height={y.bandwidth() + 12} fill="transparent" />
                  <BarPath x={0} y={by} width={bw} height={y.bandwidth()} dir="right" fill="var(--cat-1)" />
                  <text x={-12} y={by + y.bandwidth() / 2} dy="0.32em" textAnchor="end" fontSize={12} fill="var(--ink-secondary)">
                    {r.stage}
                  </text>
                  <text x={bw + 10} y={by + y.bandwidth() / 2} dy="0.32em" fontSize={11} fontFamily="var(--font-mono)" fill="var(--ink-secondary)">
                    {r.claims.toLocaleString("en-IN")}
                  </text>
                  {/* Dwell time as a separate small track, not a second y-axis. */}
                  {r.dwellDays !== null && (
                    <g transform={`translate(${iw + 32},${by + y.bandwidth() / 2})`}>
                      <line x1={0} x2={96} y1={0} y2={0} stroke="var(--grid)" strokeWidth={3} strokeLinecap="round" />
                      <line x1={0} x2={Math.max(3, dwellW)} y1={0} y2={0} stroke="var(--cat-2)" strokeWidth={3} strokeLinecap="round" />
                      <text x={104} dy="0.32em" fontSize={11} fontFamily="var(--font-mono)" fill="var(--ink-muted)">
                        {r.dwellDays.toFixed(0)}d
                      </text>
                    </g>
                  )}
                </g>
              );
            })}
          </g>
        </svg>
        <Tooltip tip={tip} width={width} />
        <ul className="legend">
          <li>
            <span className="legend__swatch" style={{ background: "var(--cat-1)" }} aria-hidden="true" />
            Claims reaching stage
          </li>
          <li>
            <span className="legend__swatch" style={{ background: "var(--cat-2)" }} aria-hidden="true" />
            Median days spent in stage
          </li>
        </ul>
      </div>
    </Figure>
  );
}
