/* Dumbbell — before/after per item.
 *
 * This replaces a grouped bar chart, and the reason is the whole point of the panel.
 * The finding is not "hospitals have a readmission rate"; it is "the rate each
 * hospital can see is lower than the real one, by this much". Grouped bars make a
 * reader compare two lengths and subtract. A dumbbell draws the difference itself —
 * the connecting segment *is* the hidden readmissions.
 *
 * One hue in two shades rather than two categorical hues: these are two measurements
 * of the same quantity, not two different series.
 */

import { scaleLinear, scalePoint } from "d3-scale";
import { useState } from "react";
import { AxisLeft, Baseline, Figure, M, Table, Tooltip, type TipState } from "./chrome";

export interface DumbbellRow {
  label: string;
  sub?: string;
  from: number;
  to: number;
  extra?: [string, string][];
}

export function Dumbbell({
  rows,
  title,
  subtitle,
  note,
  fromLabel,
  toLabel,
  format = (v: number) => v.toFixed(2),
  height = 340,
}: {
  rows: DumbbellRow[];
  title: string;
  subtitle?: string;
  note?: string;
  fromLabel: string;
  toLabel: string;
  format?: (v: number) => string;
  height?: number;
}) {
  const [tip, setTip] = useState<TipState | null>(null);
  const width = 720;
  const m = M(28, 64, 44, 168);
  const iw = width - m.left - m.right;
  const ih = height - m.top - m.bottom;

  const y = scalePoint<string>()
    .domain(rows.map((r) => r.label))
    .range([0, ih])
    .padding(0.7);

  const lo = Math.min(...rows.flatMap((r) => [r.from, r.to]));
  const hi = Math.max(...rows.flatMap((r) => [r.from, r.to]));
  const pad = (hi - lo) * 0.25 || 1;
  const x = scaleLinear()
    .domain([Math.max(0, lo - pad), hi + pad])
    .range([0, iw]);

  const ticks = x.ticks(5);

  return (
    <Figure
      title={title}
      subtitle={subtitle}
      note={note}
      table={
        <Table
          columns={["Hospital", fromLabel, toLabel, "Gap"]}
          align={["l", "r", "r", "r"]}
          rows={rows.map((r) => [
            r.label,
            format(r.from),
            format(r.to),
            `+${format(r.to - r.from)}`,
          ])}
        />
      }
    >
      <div className="plot" onMouseLeave={() => setTip(null)}>
        <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label={title}>
          <g transform={`translate(${m.left},${m.top})`}>
            {ticks.map((t) => (
              <line
                key={t}
                x1={x(t)}
                x2={x(t)}
                y1={0}
                y2={ih}
                stroke="var(--grid)"
                shapeRendering="crispEdges"
              />
            ))}
            <Baseline y={ih} x0={0} x1={iw} />
            <AxisLeft ticks={ticks} scale={(v) => v} x={0} />
            <g transform={`translate(0,${ih + 16})`}>
              {ticks.map((t) => (
                <text
                  key={t}
                  x={x(t)}
                  textAnchor="middle"
                  fill="var(--ink-muted)"
                  fontSize={11}
                  fontFamily="var(--font-mono)"
                >
                  {format(t)}
                </text>
              ))}
            </g>

            {rows.map((r) => {
              const cy = y(r.label) ?? 0;
              const x1 = x(r.from);
              const x2 = x(r.to);
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
                        [fromLabel, format(r.from)],
                        [toLabel, format(r.to)],
                        ["Difference", `+${format(r.to - r.from)}`],
                        ...(r.extra ?? []),
                      ],
                    });
                  }}
                >
                  {/* Generous invisible hit target — the marks are thin by design,
                      but the pointer should not have to be. */}
                  <rect x={0} y={cy - 14} width={iw} height={28} fill="transparent" />

                  {/* The connector IS the finding: its length is the hidden rate. */}
                  <line
                    x1={x1}
                    x2={x2}
                    y1={cy}
                    y2={cy}
                    stroke="var(--cat-1)"
                    strokeOpacity={0.28}
                    strokeWidth={6}
                    strokeLinecap="round"
                  />
                  {/* 2px surface ring so overlapping marks stay separable. */}
                  <circle cx={x1} cy={cy} r={5} fill="var(--surface)" />
                  <circle cx={x1} cy={cy} r={4.5} fill="var(--recede)" />
                  <circle cx={x2} cy={cy} r={6} fill="var(--surface)" />
                  <circle cx={x2} cy={cy} r={5} fill="var(--cat-1)" />

                  <text
                    x={-12}
                    y={cy}
                    dy="0.32em"
                    textAnchor="end"
                    fontSize={12}
                    fill="var(--ink-secondary)"
                  >
                    {r.label}
                  </text>
                  {/* Direct label on the gap — the number the panel exists to show. */}
                  <text
                    x={x2 + 12}
                    y={cy}
                    dy="0.32em"
                    fontSize={11}
                    fontFamily="var(--font-mono)"
                    fill="var(--ink-secondary)"
                  >
                    +{format(r.to - r.from)}
                  </text>
                </g>
              );
            })}
          </g>
        </svg>
        <Tooltip tip={tip} width={width} />
        <ul className="legend">
          <li>
            <span className="legend__swatch legend__swatch--ring" aria-hidden="true" />
            {fromLabel}
          </li>
          <li>
            <span
              className="legend__swatch"
              style={{ background: "var(--cat-1)" }}
              aria-hidden="true"
            />
            {toLabel}
          </li>
        </ul>
      </div>
    </Figure>
  );
}
